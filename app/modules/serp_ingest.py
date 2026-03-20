"""Обработка XML-ответов Yandex Search и сохранение релевантных результатов."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import List, Optional
from xml.etree import ElementTree as ET

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.modules.constants import EXCLUDED_DOMAINS
from app.modules.utils.db import get_session_factory, session_scope
from app.modules.utils.normalize import clean_snippet, normalize_domain, normalize_url

LOGGER = logging.getLogger("app.serp_ingest")

EXCLUDED_DOMAIN_SUFFIXES = tuple(sorted(EXCLUDED_DOMAINS))
MALL_KEYWORDS = (
    "торгов",
    "тц",
    "трц",
    "молл",
    "mall",
    "shopping center",
    "shopping centre",
    "торгово-развлекатель",
)
REAL_ESTATE_KEYWORDS = (
    "агентств",
    "недвижим",
    "риэлтор",
    "риелтор",
    "real estate",
    "estate agency",
    "property",
)
NEGATIVE_MARKERS = (
    "каталог",
    "справочник",
    "отзывы",
    "афиша",
    "объявления",
    "агрегатор",
    "listing",
    "directory",
    "marketplace",
)


def _is_excluded_domain(domain: str) -> bool:
    domain_lower = (domain or "").lower()
    return any(
        domain_lower == excluded or domain_lower.endswith(f".{excluded}")
        for excluded in EXCLUDED_DOMAIN_SUFFIXES
    )


def _normalize_text(text: str | None) -> str:
    return clean_snippet(text).lower()


def classify_document(document: "SerpDocument", expected_entity_type: str | None) -> tuple[bool, float, Optional[str]]:
    """Оценивает релевантность документа под нужный тип организации."""
    title = _normalize_text(document.title)
    snippet = _normalize_text(document.snippet)
    url = _normalize_text(document.url)
    haystack = " ".join(part for part in (title, snippet, url) if part)

    if not haystack:
        return False, 0.0, "empty_document"

    if any(marker in haystack for marker in NEGATIVE_MARKERS):
        return False, 0.0, "negative_marker"

    keywords = MALL_KEYWORDS if expected_entity_type == "mall" else REAL_ESTATE_KEYWORDS
    if not any(keyword in haystack for keyword in keywords):
        return False, 0.2, "missing_entity_keywords"

    if "официаль" in haystack:
        return True, 0.95, None
    if expected_entity_type == "mall" and any(keyword in haystack for keyword in ("торгов", "трц", "молл", "mall")):
        return True, 0.85, None
    if expected_entity_type == "real_estate_agency" and any(
        keyword in haystack for keyword in ("агентств", "недвижим", "риэлтор", "риелтор")
    ):
        return True, 0.85, None
    return True, 0.7, None


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

    def __init__(self, session_factory: Optional[sessionmaker[Session]] = None) -> None:
        self.session_factory = session_factory or get_session_factory()

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
                is_relevant, relevance_score, discard_reason = classify_document(document, entity_type)
                if not is_relevant:
                    LOGGER.debug(
                        "Документ %s отброшен: entity_type=%s reason=%s",
                        document.url,
                        entity_type,
                        discard_reason,
                    )
                    continue

                result_id = self._upsert_result(
                    session,
                    operation_db_id,
                    document,
                    entity_type=entity_type,
                    city=city,
                    relevance_score=relevance_score,
                    yandex_operation_id=yandex_operation_id,
                )
                inserted.append(result_id)
                self._ensure_company(
                    session,
                    document,
                    entity_type=entity_type,
                    city=city,
                    relevance_score=relevance_score,
                )
        return inserted

    def _upsert_result(
        self,
        session: Session,
        operation_db_id: str,
        document: SerpDocument,
        *,
        entity_type: Optional[str],
        city: Optional[str],
        relevance_score: float,
        yandex_operation_id: str | None = None,
    ) -> str:
        metadata_payload = {
            "language": document.language,
            "source": "yandex",
            "entity_type": entity_type,
            "city": city,
            "relevance_score": relevance_score,
        }
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
        relevance_score: float,
    ) -> None:
        dedupe_hash = document.domain
        attributes = json.dumps(
            {
                "source": "yandex_serp",
                "last_snippet": document.snippet,
                "entity_type": entity_type,
                "source_city": city,
                "quality_score": relevance_score,
            },
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
