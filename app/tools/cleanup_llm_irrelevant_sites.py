"""CLI для мягкого исключения нерелевантных сайтов по успешному LLM-вердикту."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import text

from app.modules.utils.db import get_session_factory, session_scope

LOGGER = logging.getLogger("app.tools.cleanup_llm_irrelevant_sites")

IRRELEVANT_VERDICTS = (
    "aggregator_or_directory",
    "mall_tenant_site",
    "developer_site",
    "media_or_article",
)

CANDIDATE_SQL = """
SELECT
    c.id AS company_id,
    c.canonical_domain,
    c.status,
    c.opt_out,
    c.attributes ->> 'llm_site_verdict' AS llm_site_verdict,
    c.attributes ->> 'llm_reason' AS llm_reason
FROM companies c
WHERE COALESCE(c.attributes ->> 'llm_status', '') = 'success'
  AND COALESCE(c.attributes ->> 'llm_site_verdict', '') = ANY(:verdicts)
  AND c.status <> 'excluded_by_llm'
ORDER BY c.updated_at ASC NULLS FIRST, c.created_at ASC NULLS FIRST
LIMIT :limit;
"""

UPDATE_COMPANY_SQL = """
UPDATE companies
SET status = 'excluded_by_llm',
    opt_out = TRUE,
    attributes = COALESCE(attributes, '{}'::jsonb) || CAST(:patch AS jsonb),
    updated_at = NOW()
WHERE id = :company_id;
"""

UPDATE_OUTREACH_SQL = """
UPDATE outreach_messages
SET status = 'skipped',
    last_error = 'excluded_by_llm',
    metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:patch AS jsonb),
    updated_at = NOW()
WHERE company_id = :company_id
  AND status = 'scheduled';
"""


@dataclass
class CleanupCandidate:
    company_id: str
    canonical_domain: str
    status: str
    opt_out: bool
    llm_site_verdict: str
    llm_reason: str | None


def _row_to_candidate(row) -> CleanupCandidate:
    data = row._mapping
    return CleanupCandidate(
        company_id=str(data["company_id"]),
        canonical_domain=str(data["canonical_domain"]),
        status=str(data["status"]),
        opt_out=bool(data["opt_out"]),
        llm_site_verdict=str(data["llm_site_verdict"]),
        llm_reason=data["llm_reason"],
    )


def _fetch_candidates(limit: int) -> list[CleanupCandidate]:
    session_factory = get_session_factory()
    with session_scope(session_factory) as session:
        rows = session.execute(
            text(CANDIDATE_SQL),
            {"limit": limit, "verdicts": list(IRRELEVANT_VERDICTS)},
        ).fetchall()
    return [_row_to_candidate(row) for row in rows]


def _build_company_patch(candidate: CleanupCandidate) -> str:
    payload = {
        "excluded_by_llm": True,
        "excluded_at": datetime.now(timezone.utc).isoformat(),
        "excluded_reason": "irrelevant_llm_site_verdict",
        "excluded_llm_site_verdict": candidate.llm_site_verdict,
        "excluded_llm_reason": candidate.llm_reason,
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_outreach_patch(candidate: CleanupCandidate) -> str:
    payload = {
        "reason": "excluded_by_llm",
        "excluded_llm_site_verdict": candidate.llm_site_verdict,
    }
    return json.dumps(payload, ensure_ascii=False)


def run(*, limit: int, dry_run: bool) -> None:
    candidates = _fetch_candidates(limit)
    LOGGER.info("Найдено кандидатов для LLM cleanup: %s", len(candidates))

    excluded = 0
    skipped = 0
    session_factory = get_session_factory()
    for index, candidate in enumerate(candidates, start=1):
        LOGGER.info(
            "LLM cleanup %s/%s: domain=%s verdict=%s status=%s opt_out=%s",
            index,
            len(candidates),
            candidate.canonical_domain,
            candidate.llm_site_verdict,
            candidate.status,
            candidate.opt_out,
        )
        if dry_run:
            skipped += 1
            continue

        company_patch = _build_company_patch(candidate)
        outreach_patch = _build_outreach_patch(candidate)
        with session_scope(session_factory) as session:
            session.execute(
                text(UPDATE_COMPANY_SQL),
                {"company_id": candidate.company_id, "patch": company_patch},
            )
            session.execute(
                text(UPDATE_OUTREACH_SQL),
                {"company_id": candidate.company_id, "patch": outreach_patch},
            )
        excluded += 1

    LOGGER.info(
        "LLM cleanup завершён: processed=%s excluded=%s skipped=%s dry_run=%s",
        len(candidates),
        excluded,
        skipped,
        dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Мягко исключает из дальнейшей работы сайты с нерелевантным успешным LLM-вердиктом."
    )
    parser.add_argument("--limit", type=int, default=500, help="Максимум компаний за один запуск.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Не писать изменения в БД, только показать кандидатов.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
