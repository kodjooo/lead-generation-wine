"""CLI для повторной LLM-классификации уже сохраненных сайтов."""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.modules.serp_ingest import SerpDocument, SerpIngestService
from app.modules.utils.db import get_session_factory, session_scope

LOGGER = logging.getLogger("app.tools.recheck_llm_sites")
DEFAULT_LOCK_TIMEOUT_MS = 1000

CANDIDATE_SQL = """
SELECT
    c.id AS company_id,
    c.canonical_domain,
    c.website_url,
    c.industry,
    c.region,
    c.actual_region,
    c.attributes ->> 'homepage_excerpt' AS homepage_excerpt,
    c.attributes ->> 'last_snippet' AS company_snippet,
    sr.id AS serp_result_id,
    sr.url AS result_url,
    sr.title AS result_title,
    sr.snippet AS result_snippet,
    sr.position AS result_position,
    COALESCE(sr.language, 'ru') AS result_language,
    COALESCE(sr.metadata ->> 'expected_city', sr.metadata ->> 'city', c.region) AS expected_city
FROM companies c
LEFT JOIN LATERAL (
    SELECT
        id,
        url,
        title,
        snippet,
        position,
        language,
        metadata
    FROM serp_results
    WHERE domain = c.canonical_domain
    ORDER BY
        CASE
            WHEN COALESCE(metadata ->> 'expected_city', metadata ->> 'city', '') = COALESCE(c.region, '') THEN 0
            ELSE 1
        END,
        position ASC,
        id DESC
    LIMIT 1
) sr ON TRUE
WHERE c.industry IN ('mall', 'real_estate_agency')
  AND COALESCE(c.attributes ->> 'homepage_excerpt', '') <> ''
  AND (
        c.attributes ->> 'llm_status' IS NULL
        OR c.attributes ->> 'llm_status' = ''
        OR (:retry_errors = TRUE AND c.attributes ->> 'llm_status' = 'error')
  )
ORDER BY c.updated_at ASC NULLS FIRST, c.created_at ASC NULLS FIRST
LIMIT :limit;
"""

UPDATE_COMPANY_SQL = """
UPDATE companies
SET
    actual_region = COALESCE(:actual_region, actual_region),
    attributes = COALESCE(attributes, '{}'::jsonb) || CAST(:patch AS jsonb),
    updated_at = NOW()
WHERE id = :company_id;
"""

UPDATE_SERP_RESULT_SQL = """
UPDATE serp_results
SET metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:patch AS jsonb)
WHERE id = :serp_result_id;
"""


@dataclass
class RecheckCandidate:
    company_id: str
    canonical_domain: str
    website_url: Optional[str]
    industry: Optional[str]
    region: Optional[str]
    actual_region: Optional[str]
    homepage_excerpt: str
    company_snippet: Optional[str]
    serp_result_id: Optional[str]
    result_url: Optional[str]
    result_title: Optional[str]
    result_snippet: Optional[str]
    result_position: Optional[int]
    result_language: Optional[str]
    expected_city: Optional[str]


def _row_to_candidate(row) -> RecheckCandidate:
    data = row._mapping
    return RecheckCandidate(
        company_id=str(data["company_id"]),
        canonical_domain=str(data["canonical_domain"]),
        website_url=data["website_url"],
        industry=data["industry"],
        region=data["region"],
        actual_region=data["actual_region"],
        homepage_excerpt=str(data["homepage_excerpt"]),
        company_snippet=data["company_snippet"],
        serp_result_id=str(data["serp_result_id"]) if data["serp_result_id"] is not None else None,
        result_url=data["result_url"],
        result_title=data["result_title"],
        result_snippet=data["result_snippet"],
        result_position=int(data["result_position"]) if data["result_position"] is not None else None,
        result_language=data["result_language"],
        expected_city=data["expected_city"],
    )


def _fetch_candidates(limit: int, *, retry_errors: bool) -> list[RecheckCandidate]:
    session_factory = get_session_factory()
    with session_scope(session_factory) as session:
        rows = session.execute(
            text(CANDIDATE_SQL),
            {"limit": limit, "retry_errors": retry_errors},
        ).fetchall()
    return [_row_to_candidate(row) for row in rows]


def _build_document(candidate: RecheckCandidate) -> SerpDocument:
    url = candidate.result_url or candidate.website_url or f"https://{candidate.canonical_domain}/"
    title = candidate.result_title or candidate.canonical_domain
    snippet = candidate.result_snippet or candidate.company_snippet or ""
    position = candidate.result_position or 1
    return SerpDocument(
        url=url,
        domain=candidate.canonical_domain,
        title=title,
        snippet=snippet,
        position=position,
        language=candidate.result_language or "ru",
    )


def _build_patch(service: SerpIngestService, candidate: RecheckCandidate) -> tuple[str, str, Optional[str]]:
    classification = service._request_site_classification_llm(
        expected_city=candidate.expected_city or candidate.region,
        expected_entity_type=candidate.industry,
        document=_build_document(candidate),
        homepage_content=candidate.homepage_excerpt,
        serp_decision=None,
        homepage_decision=None,
    )
    patch = service._build_llm_tracking_payload(classification)
    if classification.detected_city is not None:
        patch["detected_city"] = classification.detected_city
        patch["city_match_source"] = "llm_recheck"
    patch["llm_recheck"] = True
    patch_json = json.dumps(patch, ensure_ascii=False)
    return patch_json, patch["llm_status"], classification.detected_city


def _is_retryable_lock_error(error: OperationalError) -> bool:
    message = str(error).lower()
    return "lock timeout" in message or "deadlock detected" in message


def _apply_patch(
    candidate: RecheckCandidate,
    patch_json: str,
    actual_region: Optional[str],
    *,
    lock_timeout_ms: int,
) -> bool:
    session_factory = get_session_factory()
    try:
        with session_scope(session_factory) as session:
            session.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout_ms}ms'"))
            session.execute(
                text(UPDATE_COMPANY_SQL),
                {
                    "company_id": candidate.company_id,
                    "actual_region": actual_region,
                    "patch": patch_json,
                },
            )
            if candidate.serp_result_id:
                session.execute(
                    text(UPDATE_SERP_RESULT_SQL),
                    {
                        "serp_result_id": candidate.serp_result_id,
                        "patch": patch_json,
                    },
                )
    except OperationalError as exc:
        if _is_retryable_lock_error(exc):
            LOGGER.warning(
                "Пропускаем LLM recheck для domain=%s из-за блокировки БД: %s",
                candidate.canonical_domain,
                exc,
            )
            return False
        raise
    return True


def run(
    *,
    limit: int,
    retry_errors: bool,
    dry_run: bool,
    lock_timeout_ms: int,
    sleep_seconds: float,
) -> None:
    service = SerpIngestService()
    candidates = _fetch_candidates(limit, retry_errors=retry_errors)
    LOGGER.info("Найдено кандидатов для LLM recheck: %s", len(candidates))

    success = 0
    errors = 0
    skipped = 0
    for index, candidate in enumerate(candidates, start=1):
        patch_json, llm_status, actual_region = _build_patch(service, candidate)
        LOGGER.info(
            "LLM recheck %s/%s: domain=%s status=%s actual_region=%s",
            index,
            len(candidates),
            candidate.canonical_domain,
            llm_status,
            actual_region,
        )
        if not dry_run:
            applied = _apply_patch(
                candidate,
                patch_json,
                actual_region,
                lock_timeout_ms=lock_timeout_ms,
            )
            if not applied:
                skipped += 1
                continue
        if llm_status == "success":
            success += 1
        else:
            errors += 1
        if sleep_seconds > 0 and index < len(candidates):
            time.sleep(sleep_seconds)

    LOGGER.info(
        "LLM recheck завершён: processed=%s success=%s error=%s skipped=%s dry_run=%s",
        len(candidates),
        success,
        errors,
        skipped,
        dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Повторно прогоняет сохранённые сайты через LLM classification.")
    parser.add_argument("--limit", type=int, default=500, help="Максимум компаний за один запуск.")
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Повторно брать записи, уже помеченные llm_status=error.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Не записывать результат в БД, только показать кандидатов и ответы.",
    )
    parser.add_argument(
        "--lock-timeout-ms",
        type=int,
        default=DEFAULT_LOCK_TIMEOUT_MS,
        help="Сколько ждать блокировку UPDATE перед skip.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Пауза между LLM-вызовами для обхода rate limit.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    run(
        limit=args.limit,
        retry_errors=args.retry_errors,
        dry_run=args.dry_run,
        lock_timeout_ms=args.lock_timeout_ms,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()
