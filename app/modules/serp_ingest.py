"""Обработка XML-ответов Yandex Search и сохранение релевантных результатов."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.modules.constants import EXCLUDED_DOMAINS
from app.config import get_settings
from app.modules.utils.db import get_session_factory, session_scope
from app.modules.utils.normalize import clean_snippet, normalize_domain, normalize_url

LOGGER = logging.getLogger("app.serp_ingest")
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

EXCLUDED_DOMAIN_SUFFIXES = tuple(sorted(EXCLUDED_DOMAINS))
COMMON_NEGATIVE_MARKERS = (
    "каталог",
    "справочник",
    "отзывы",
    "афиша",
    "объявления",
    "агрегатор",
    "listing",
    "directory",
    "marketplace",
    "рейтинг",
    "подборка",
    "лучшие",
    "список",
)
AGGREGATOR_URL_PATTERNS = (
    "/catalog",
    "/rating",
    "/ratings",
    "/reviews",
    "/review",
    "/city/",
    "/cities/",
    "/mall/",
    "/malls/",
    "/shopping-centers/",
    "/shopping-centre/",
    "/torgovye-centry/",
    "/agentstva-nedvizhimosti/",
    "/agency/",
    "/agencies/",
    "/companies/",
    "/objects/",
    "/list/",
)
MALL_IDENTITY_MARKERS = (
    "торговый центр",
    "торгово-развлекательный центр",
    "трц",
    "тц",
    "трк",
    "молл",
    "mall",
)
MALL_OPERATIONAL_MARKERS = (
    "магазин",
    "время работы",
    "как добраться",
    "арендаторам",
    "аренда",
    "развлечен",
    "фудкорт",
    "кино",
    "схема",
    "контакты",
    "парковк",
    "красная площадь",
    "red square",
)
MALL_NEGATIVE_MARKERS = (
    "все торговые центры",
    "каталог торговых центров",
    "рейтинг торговых центров",
    "торговые центры краснодара",
    "подборка торговых центров",
)
AGENCY_IDENTITY_MARKERS = (
    "агентство недвижимости",
    "риэлтор",
    "риелтор",
    "риэлт",
    "риелт",
    "недвижимость",
)
AGENCY_OPERATIONAL_MARKERS = (
    "объекты",
    "новострой",
    "ипотека",
    "купить",
    "продать",
    "аренда",
    "квартир",
    "дом",
    "коммерческая недвижимость",
    "оставить заявку",
    "подобрать",
    "контакты",
)
AGENCY_NEGATIVE_MARKERS = (
    "лучшие агентства",
    "рейтинг агентств",
    "список агентств",
    "каталог агентств",
    "объявления",
)
AGENCY_DEVELOPER_MARKERS = (
    "застройщик",
    "девелоп",
    "development",
    "жилой комплекс",
    "жк ",
    "жк.",
    "квартиры от застройщика",
    "новостройки от застройщика",
    "строительная компания",
)
MALL_DOMAIN_MARKERS = (
    "mall",
    "mol",
    "trc",
    "trk",
    "tc",
    "center",
    "centr",
    "gorod",
    "plaza",
    "gal",
    "square",
    "red-square",
)
AGENCY_DOMAIN_MARKERS = (
    "realty",
    "real",
    "rielt",
    "rieltor",
    "rielty",
    "agency",
    "estate",
    "nedvizh",
    "kvart",
    "realt",
)
KNOWN_RU_CITIES = (
    "Москва",
    "Санкт-Петербург",
    "Краснодар",
    "Сочи",
    "Ростов-на-Дону",
    "Казань",
    "Екатеринбург",
    "Новосибирск",
    "Нижний Новгород",
    "Самара",
    "Воронеж",
    "Уфа",
    "Пермь",
    "Челябинск",
    "Омск",
    "Красноярск",
    "Волгоград",
    "Тюмень",
    "Ижевск",
    "Ставрополь",
    "Новороссийск",
    "Анапа",
    "Геленджик",
)


def _is_excluded_domain(domain: str) -> bool:
    domain_lower = (domain or "").lower()
    return any(
        domain_lower == excluded or domain_lower.endswith(f".{excluded}")
        for excluded in EXCLUDED_DOMAIN_SUFFIXES
    )


def _normalize_text(text: str | None) -> str:
    return clean_snippet(text).lower()


def _contains_any(haystack: str, needles: tuple[str, ...]) -> bool:
    return any(needle in haystack for needle in needles)


def _score_hits(haystack: str, needles: tuple[str, ...], weight: float) -> float:
    return sum(weight for needle in needles if needle in haystack)


@dataclass
class ScreeningDecision:
    """Результат фильтрации кандидата."""

    is_relevant: bool
    score: float
    reason: Optional[str] = None
    requires_verification: bool = False


class SerpParseError(RuntimeError):
    """Ошибка парсинга XML-ответа."""


@dataclass
class SerpDocument:
    """Нормализованный документ выдачи."""

    url: str
    domain: str
    title: str
    snippet: str
    position: int
    language: Optional[str]


@dataclass
class CityDetection:
    """Результат определения фактического города сайта."""

    detected_city: Optional[str]
    score: float
    source: Optional[str]


@dataclass
class SiteClassificationDecision:
    """Результат комплексной LLM-классификации сайта."""

    site_verdict: Optional[str]
    detected_city: Optional[str]
    confidence: float
    reason: Optional[str]


def evaluate_serp_document(document: "SerpDocument", expected_entity_type: str | None) -> ScreeningDecision:
    """Предварительно оценивает документ по данным SERP."""
    title = _normalize_text(document.title)
    snippet = _normalize_text(document.snippet)
    url = _normalize_text(document.url)
    domain = _normalize_text(document.domain)
    haystack = " ".join(part for part in (title, snippet, url) if part)

    if not haystack:
        return ScreeningDecision(False, 0.0, "empty_document")
    if _contains_any(haystack, COMMON_NEGATIVE_MARKERS):
        return ScreeningDecision(False, 0.0, "negative_marker")
    if _contains_any(url, AGGREGATOR_URL_PATTERNS):
        return ScreeningDecision(False, 0.0, "aggregator_url_pattern")

    score = 0.0
    if "официаль" in haystack:
        score += 3.0

    if expected_entity_type == "mall":
        identity_score = _score_hits(haystack, MALL_IDENTITY_MARKERS, 2.0)
        operational_score = _score_hits(haystack, MALL_OPERATIONAL_MARKERS, 1.2)
        negative_score = _score_hits(haystack, MALL_NEGATIVE_MARKERS, 3.5)
        domain_score = _score_hits(domain, MALL_DOMAIN_MARKERS, 1.0)
        score += identity_score + operational_score + domain_score - negative_score
        if identity_score <= 0 or score < 2.5:
            return ScreeningDecision(True, round(score, 2), "serp_needs_homepage_verification", True)
        return ScreeningDecision(True, round(score, 2), None)

    if expected_entity_type == "real_estate_agency":
        identity_score = _score_hits(haystack, AGENCY_IDENTITY_MARKERS, 2.0)
        operational_score = _score_hits(haystack, AGENCY_OPERATIONAL_MARKERS, 1.2)
        negative_score = _score_hits(haystack, AGENCY_NEGATIVE_MARKERS, 3.5)
        developer_score = _score_hits(haystack, AGENCY_DEVELOPER_MARKERS, 2.5)
        domain_score = _score_hits(domain, AGENCY_DOMAIN_MARKERS, 1.0)
        score += identity_score + operational_score + domain_score - negative_score - developer_score
        if identity_score <= 0 or score < 2.5:
            return ScreeningDecision(True, round(score, 2), "serp_needs_homepage_verification", True)
        return ScreeningDecision(True, round(score, 2), None)

    return ScreeningDecision(False, 0.0, "unknown_entity_type")


def evaluate_homepage_content(content: str, entity_type: str | None) -> ScreeningDecision:
    """Финально подтверждает, что домен похож на официальный сайт нужного типа."""
    haystack = _normalize_text(content)
    if not haystack:
        return ScreeningDecision(False, 0.0, "empty_homepage")
    if _contains_any(haystack, COMMON_NEGATIVE_MARKERS):
        return ScreeningDecision(False, 0.0, "homepage_negative_marker")

    if entity_type == "mall":
        identity_score = _score_hits(haystack, MALL_IDENTITY_MARKERS, 2.0)
        operational_score = _score_hits(haystack, MALL_OPERATIONAL_MARKERS, 1.5)
        negative_score = _score_hits(haystack, MALL_NEGATIVE_MARKERS, 4.0)
        score = identity_score + operational_score - negative_score
        if identity_score < 2.0:
            return ScreeningDecision(False, score, "homepage_missing_mall_identity")
        if operational_score < 1.5 and score < 3.5:
            return ScreeningDecision(False, score, "homepage_missing_mall_operational_markers")
        if score < 3.5:
            return ScreeningDecision(False, score, "homepage_low_score")
        return ScreeningDecision(True, round(score, 2), None)

    if entity_type == "real_estate_agency":
        identity_score = _score_hits(haystack, AGENCY_IDENTITY_MARKERS, 2.0)
        operational_score = _score_hits(haystack, AGENCY_OPERATIONAL_MARKERS, 1.5)
        negative_score = _score_hits(haystack, AGENCY_NEGATIVE_MARKERS, 4.0)
        developer_score = _score_hits(haystack, AGENCY_DEVELOPER_MARKERS, 3.0)
        score = identity_score + operational_score - negative_score - developer_score
        if developer_score >= 3.0 and identity_score < 2.0:
            return ScreeningDecision(False, score, "homepage_developer_site")
        if identity_score < 2.0:
            return ScreeningDecision(False, score, "homepage_missing_agency_identity")
        if operational_score < 1.5 and score < 3.5:
            return ScreeningDecision(False, score, "homepage_missing_agency_operational_markers")
        if score < 3.5:
            return ScreeningDecision(False, score, "homepage_low_score")
        return ScreeningDecision(True, round(score, 2), None)

    return ScreeningDecision(False, 0.0, "unknown_entity_type")


def _city_pattern(city: str) -> re.Pattern[str]:
    normalized = re.escape(_normalize_text(city))
    return re.compile(rf"(?<![a-zа-я0-9]){normalized}(?![a-zа-я0-9])")


def detect_actual_city(
    *,
    expected_city: str | None,
    document: SerpDocument,
    homepage_content: str | None,
) -> CityDetection:
    """Определяет фактический город по SERP и homepage-контенту."""
    candidates: list[str] = []
    if expected_city:
        candidates.append(expected_city)
    for city in KNOWN_RU_CITIES:
        if city not in candidates:
            candidates.append(city)

    serp_haystack = _normalize_text(" ".join((document.title, document.snippet, document.url)))
    homepage_haystack = _normalize_text(homepage_content)
    best_city: Optional[str] = None
    best_score = 0.0
    best_source: Optional[str] = None

    for city in candidates:
        pattern = _city_pattern(city)
        score = 0.0
        source: Optional[str] = None

        if homepage_haystack and pattern.search(homepage_haystack):
            score += 3.0
            source = "homepage"
            if f"г { _normalize_text(city) }" in homepage_haystack or f"город { _normalize_text(city) }" in homepage_haystack:
                score += 1.0

        if serp_haystack and pattern.search(serp_haystack):
            score += 1.5
            source = source or "serp"

        if score > best_score:
            best_city = city
            best_score = score
            best_source = source

    if best_score <= 0:
        return CityDetection(None, 0.0, None)
    return CityDetection(best_city, round(best_score, 2), best_source)


def parse_serp_xml(xml_payload: bytes) -> List[SerpDocument]:
    """Извлекает документы из XML-ответа Yandex Search."""
    if not xml_payload:
        return []

    try:
        root = ET.fromstring(xml_payload)
    except ET.ParseError as exc:
        raise SerpParseError("Некорректный XML выдачи.") from exc

    documents: List[SerpDocument] = []
    for position, doc in enumerate(root.findall(".//doc"), start=1):
        url_text = (doc.findtext("url") or doc.findtext("lurl") or "").strip()
        normalized_url = normalize_url(url_text)
        if not normalized_url:
            LOGGER.debug("Пропущен документ без корректного URL: %s", url_text)
            continue

        domain_text = doc.findtext("domain") or ""
        normalized_domain = normalize_domain(domain_text or normalized_url)
        title = (doc.findtext("title") or doc.findtext("name") or normalized_domain).strip()
        passages = [clean_snippet(node.text) for node in doc.findall(".//passages/passage")]
        snippet = clean_snippet(" ".join(filter(None, passages)))

        language = None
        for prop in doc.findall(".//properties/property"):
            if prop.get("name") == "lang" and prop.text:
                language = prop.text.strip()
                break

        documents.append(
            SerpDocument(
                url=normalized_url,
                domain=normalized_domain,
                title=title,
                snippet=snippet,
                position=position,
                language=language,
            )
        )
    return documents


INSERT_SERP_RESULT_SQL = """
INSERT INTO serp_results (
    operation_id,
    url,
    domain,
    title,
    snippet,
    position,
    language,
    is_processed,
    metadata
)
VALUES (
    :operation_id,
    :url,
    :domain,
    :title,
    :snippet,
    :position,
    :language,
    TRUE,
    CAST(:metadata AS JSONB)
)
ON CONFLICT (operation_id, url)
DO UPDATE SET
    title = EXCLUDED.title,
    snippet = EXCLUDED.snippet,
    position = EXCLUDED.position,
    language = EXCLUDED.language,
    is_processed = TRUE,
    metadata = serp_results.metadata || EXCLUDED.metadata
RETURNING id;
"""


UPSERT_COMPANY_SQL = """
INSERT INTO companies (
    name,
    canonical_domain,
    website_url,
    industry,
    region,
    status,
    dedupe_hash,
    attributes,
    source,
    first_seen_at,
    last_seen_at
)
VALUES (
    :name,
    :domain,
    :website_url,
    :industry,
    :region,
    'new',
    :dedupe_hash,
    CAST(:attributes AS JSONB),
    'yandex_search_api',
    NOW(),
    NOW()
)
ON CONFLICT (dedupe_hash)
DO UPDATE SET
    website_url = COALESCE(companies.website_url, EXCLUDED.website_url),
    industry = COALESCE(companies.industry, EXCLUDED.industry),
    region = COALESCE(companies.region, EXCLUDED.region),
    attributes = companies.attributes || EXCLUDED.attributes,
    last_seen_at = NOW(),
    updated_at = NOW()
RETURNING id;
"""


class SerpIngestService:
    """Сохраняет релевантные документы выдачи в БД."""

    def __init__(
        self,
        session_factory: Optional[sessionmaker[Session]] = None,
        *,
        timeout: float = 8.0,
    ) -> None:
        self.settings = get_settings()
        self.session_factory = session_factory or get_session_factory()
        self.timeout = timeout
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
            "Cache-Control": "no-cache",
        }
        self._homepage_cache: Dict[tuple[str, str], ScreeningDecision] = {}
        self._homepage_content_cache: Dict[str, str] = {}
        self._llm_classification_cache: Dict[tuple[str, str, str], SiteClassificationDecision] = {}

    def ingest(
        self,
        operation_db_id: str,
        xml_payload: bytes,
        *,
        yandex_operation_id: str | None = None,
        query_metadata: Optional[dict] = None,
    ) -> List[str]:
        """Парсит и сохраняет только релевантные результаты выдачи."""
        documents = parse_serp_xml(xml_payload)
        if not documents:
            LOGGER.info("Операция %s не содержит документов для сохранения.", operation_db_id)
            return []

        query_metadata = query_metadata or {}
        entity_type = query_metadata.get("entity_type")
        city = query_metadata.get("city")
        inserted: List[str] = []
        with session_scope(self.session_factory) as session:
            for document in documents:
                if _is_excluded_domain(document.domain):
                    continue

                serp_decision = evaluate_serp_document(document, entity_type)
                if not serp_decision.is_relevant:
                    LOGGER.debug(
                        "Документ %s отброшен на этапе SERP: entity_type=%s reason=%s",
                        document.url,
                        entity_type,
                        serp_decision.reason,
                    )
                    continue

                homepage_content = self._get_homepage_content(document.domain)
                homepage_decision = self._verify_candidate_homepage(document.domain, entity_type)
                if not homepage_decision.is_relevant:
                    LOGGER.debug(
                        "Документ %s отброшен на этапе homepage verification: entity_type=%s reason=%s",
                        document.url,
                        entity_type,
                        homepage_decision.reason,
                    )
                    continue

                city_detection = detect_actual_city(
                    expected_city=city,
                    document=document,
                    homepage_content=homepage_content,
                )
                llm_classification = self._maybe_classify_site_with_llm(
                    expected_city=city,
                    expected_entity_type=entity_type,
                    document=document,
                    homepage_content=homepage_content,
                    detection=city_detection,
                )
                if llm_classification and not self._is_llm_verdict_accepted(entity_type, llm_classification.site_verdict):
                    LOGGER.debug(
                        "Документ %s отброшен по LLM-классификации: entity_type=%s verdict=%s confidence=%s",
                        document.url,
                        entity_type,
                        llm_classification.site_verdict,
                        llm_classification.confidence,
                    )
                    continue
                if llm_classification and llm_classification.detected_city:
                    city_detection = CityDetection(
                        detected_city=llm_classification.detected_city,
                        score=round(llm_classification.confidence * 5.0, 2),
                        source="llm",
                    )
                final_score = round((serp_decision.score + homepage_decision.score) / 2.0, 2)
                result_id = self._upsert_result(
                    session,
                    operation_db_id,
                    document,
                    entity_type=entity_type,
                    city=city,
                    city_detection=city_detection,
                    relevance_score=final_score,
                    screening_reason=homepage_decision.reason or serp_decision.reason,
                    llm_classification=llm_classification,
                    yandex_operation_id=yandex_operation_id,
                )
                inserted.append(result_id)
                self._ensure_company(
                    session,
                    document,
                    entity_type=entity_type,
                    city=city,
                    city_detection=city_detection,
                    relevance_score=final_score,
                    llm_classification=llm_classification,
                )
        return inserted

    def _verify_candidate_homepage(self, domain: str, entity_type: Optional[str]) -> ScreeningDecision:
        cache_key = (domain, entity_type or "")
        if cache_key in self._homepage_cache:
            return self._homepage_cache[cache_key]

        homepage_url = normalize_url(f"https://{domain}")
        html = self._fetch_homepage(homepage_url)
        if not html:
            http_url = normalize_url(f"http://{domain}")
            html = self._fetch_homepage(http_url)
        if not html:
            decision = ScreeningDecision(False, 0.0, "homepage_unreachable")
            self._homepage_cache[cache_key] = decision
            return decision

        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        text_content = soup.get_text(" ", strip=True)
        content = " ".join(part for part in (title, text_content[:12000]) if part)
        decision = evaluate_homepage_content(content, entity_type)
        self._homepage_cache[cache_key] = decision
        return decision

    def _get_homepage_content(self, domain: str) -> str:
        if domain in self._homepage_content_cache:
            return self._homepage_content_cache[domain]

        homepage_url = normalize_url(f"https://{domain}")
        html = self._fetch_homepage(homepage_url)
        if not html:
            http_url = normalize_url(f"http://{domain}")
            html = self._fetch_homepage(http_url)
        if not html:
            self._homepage_content_cache[domain] = ""
            return ""

        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        text_content = soup.get_text(" ", strip=True)
        content = " ".join(part for part in (title, text_content[:12000]) if part)
        self._homepage_content_cache[domain] = content
        return content

    def _fetch_homepage(self, url: str) -> str:
        try:
            with httpx.Client(timeout=self.timeout, headers=self.headers, follow_redirects=True) as client:
                response = client.get(url)
            if response.status_code >= 400:
                return ""
            return response.text
        except httpx.HTTPError:
            return ""

    def _maybe_classify_site_with_llm(
        self,
        *,
        expected_city: str | None,
        expected_entity_type: str | None,
        document: SerpDocument,
        homepage_content: str,
        detection: CityDetection,
    ) -> SiteClassificationDecision | None:
        if not self.settings.site_classification_llm_enabled:
            return None
        if not self.settings.openai_api_key:
            return None
        if detection.detected_city and detection.score >= 3.0:
            return None
        cache_key = (
            expected_entity_type or "",
            document.domain,
            homepage_content[:1000],
        )
        if cache_key in self._llm_classification_cache:
            return self._llm_classification_cache[cache_key]

        llm_decision = self._request_site_classification_llm(
            expected_city=expected_city,
            expected_entity_type=expected_entity_type,
            document=document,
            homepage_content=homepage_content,
        )
        self._llm_classification_cache[cache_key] = llm_decision
        if llm_decision.confidence < self.settings.site_classification_llm_min_confidence:
            return None
        return llm_decision

    def _request_site_classification_llm(
        self,
        *,
        expected_city: str | None,
        expected_entity_type: str | None,
        document: SerpDocument,
        homepage_content: str,
    ) -> SiteClassificationDecision:
        payload = {
            "model": self.settings.site_classification_llm_model,
            "temperature": 0,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "SiteClassification",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "site_verdict": {
                                "type": ["string", "null"],
                                "enum": [
                                    "official_mall_site",
                                    "mall_tenant_site",
                                    "official_real_estate_agency_site",
                                    "developer_site",
                                    "aggregator_or_directory",
                                    "media_or_article",
                                    "uncertain",
                                    None,
                                ],
                            },
                            "detected_city": {"type": ["string", "null"]},
                            "confidence": {"type": "number"},
                            "reason": {"type": ["string", "null"]},
                        },
                        "required": ["site_verdict", "detected_city", "confidence", "reason"],
                    },
                },
            },
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Определи тип сайта и фактический город по ограниченному контексту. "
                        "Сайт может быть официальным сайтом торгового центра, сайтом арендатора внутри ТЦ, "
                        "официальным сайтом агентства недвижимости, сайтом застройщика, агрегатором, медиа-страницей "
                        "или неопределённым случаем. Если уверенности нет, верни verdict=uncertain и detected_city=null. "
                        "Опирайся только на переданный контекст."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "expected_city": expected_city,
                            "expected_entity_type": expected_entity_type,
                            "serp": {
                                "title": document.title,
                                "snippet": document.snippet,
                                "url": document.url,
                            },
                            "homepage_excerpt": homepage_content[:5000],
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(OPENAI_CHAT_COMPLETIONS_URL, headers=headers, json=payload)
                response.raise_for_status()
            body = response.json()
            content = body["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            verdict = parsed.get("site_verdict") or None
            city = parsed.get("detected_city") or None
            confidence = float(parsed.get("confidence") or 0.0)
            reason = parsed.get("reason") or None
            return SiteClassificationDecision(
                site_verdict=verdict,
                detected_city=city,
                confidence=confidence,
                reason=reason,
            )
        except (httpx.HTTPError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return SiteClassificationDecision(
                site_verdict=None,
                detected_city=None,
                confidence=0.0,
                reason=None,
            )

    def _is_llm_verdict_accepted(self, entity_type: str | None, verdict: str | None) -> bool:
        if not verdict:
            return True
        if entity_type == "mall":
            return verdict == "official_mall_site"
        if entity_type == "real_estate_agency":
            return verdict == "official_real_estate_agency_site"
        return verdict not in {"aggregator_or_directory", "media_or_article", "mall_tenant_site", "developer_site"}

    def _upsert_result(
        self,
        session: Session,
        operation_db_id: str,
        document: SerpDocument,
        *,
        entity_type: Optional[str],
        city: Optional[str],
        city_detection: CityDetection,
        relevance_score: float,
        screening_reason: str | None,
        llm_classification: SiteClassificationDecision | None,
        yandex_operation_id: str | None = None,
    ) -> str:
        metadata_payload = {
            "language": document.language,
            "source": "yandex",
            "entity_type": entity_type,
            "city": city,
            "expected_city": city,
            "detected_city": city_detection.detected_city,
            "city_match_score": city_detection.score,
            "city_match_source": city_detection.source,
            "relevance_score": relevance_score,
            "screening_reason": screening_reason,
        }
        if llm_classification:
            metadata_payload["llm_site_verdict"] = llm_classification.site_verdict
            metadata_payload["llm_confidence"] = llm_classification.confidence
            metadata_payload["llm_reason"] = llm_classification.reason
        if yandex_operation_id:
            metadata_payload["yandex_operation_id"] = yandex_operation_id

        result = session.execute(
            text(INSERT_SERP_RESULT_SQL),
            {
                "operation_id": operation_db_id,
                "url": document.url,
                "domain": document.domain,
                "title": document.title,
                "snippet": document.snippet,
                "position": document.position,
                "language": document.language,
                "metadata": json.dumps(metadata_payload, ensure_ascii=False),
            },
        )
        return str(result.scalar_one())

    def _ensure_company(
        self,
        session: Session,
        document: SerpDocument,
        *,
        entity_type: Optional[str],
        city: Optional[str],
        city_detection: CityDetection,
        relevance_score: float,
        llm_classification: SiteClassificationDecision | None,
    ) -> None:
        dedupe_hash = document.domain
        attributes_payload = {
            "source": "yandex_serp",
            "last_snippet": document.snippet,
            "entity_type": entity_type,
            "source_city": city,
            "expected_city": city,
            "detected_city": city_detection.detected_city,
            "city_match_score": city_detection.score,
            "city_match_source": city_detection.source,
            "quality_score": relevance_score,
        }
        if llm_classification:
            attributes_payload["llm_site_verdict"] = llm_classification.site_verdict
            attributes_payload["llm_confidence"] = llm_classification.confidence
            attributes_payload["llm_reason"] = llm_classification.reason
        attributes = json.dumps(
            attributes_payload,
            ensure_ascii=False,
        )
        session.execute(
            text(UPSERT_COMPANY_SQL),
            {
                "name": document.title or document.domain,
                "domain": document.domain or None,
                "website_url": document.url,
                "industry": entity_type,
                "region": city,
                "dedupe_hash": dedupe_hash,
                "attributes": attributes,
            },
        )
