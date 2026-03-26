"""Дедупликация компаний и доменов."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.modules.utils.db import get_session_factory, session_scope
from app.modules.utils.normalize import build_company_dedupe_key, normalize_domain

LOGGER = logging.getLogger("app.deduplicate")


@dataclass
class DeduplicationStats:
    """Статистика выполнения дедупликации."""

    hash_updates: int = 0
    duplicates_marked: int = 0
    primary_companies: int = 0
    updated_records: int = 0


class DeduplicationService:
    """Инкапсулирует дедупликацию компаний по домену/названию."""

    def __init__(self, session_factory: Optional[sessionmaker[Session]] = None) -> None:
        self.session_factory = session_factory or get_session_factory()

    def run(self, session: Optional[Session] = None) -> DeduplicationStats:
        """Запускает дедупликацию в рамках транзакции."""
        if session is not None:
            return self._run_with_session(session)

        with session_scope(self.session_factory) as scoped_session:
            stats = self._run_with_session(scoped_session)
        return stats

    def _run_with_session(self, session: Session) -> DeduplicationStats:
        stats = DeduplicationStats()
        stats.hash_updates = self._refresh_dedupe_hashes(session)
        primary_ids, duplicate_ids = self._group_duplicates(session)
        stats.primary_companies = len(primary_ids)
        stats.duplicates_marked = len(duplicate_ids)
        stats.updated_records = self._apply_duplicate_updates(session, primary_ids, duplicate_ids)
        return stats

    def _refresh_dedupe_hashes(self, session: Session) -> int:
        """Пересчитывает dedupe_hash на основе нормализованных доменов."""
        rows = list(
            session.execute(
                text(
                    """
                    SELECT id, canonical_domain, website_url, dedupe_hash
                    FROM companies
                    """
                )
            ).mappings()
        )

        updates = 0
        for row in rows:
            domain_source = row["canonical_domain"] or row["website_url"] or str(row["id"])
            dedupe_hash = build_company_dedupe_key(None, domain_source)
            if dedupe_hash != (row["dedupe_hash"] or ""):
                session.execute(
                    text(
                        """
                        UPDATE companies
                        SET dedupe_hash = :dedupe_hash,
                            canonical_domain = :canonical_domain,
                            updated_at = NOW()
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": row["id"],
                        "dedupe_hash": dedupe_hash,
                        "canonical_domain": normalize_domain(domain_source),
                    },
                )
                updates += 1
        if updates:
            LOGGER.info("Обновлено %s dedupe_hash значений.", updates)
        return updates

    def _group_duplicates(self, session: Session) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Формирует словари primary/duplicate id по dedupe_hash."""
        rows = list(
            session.execute(
                text(
                    """
                    SELECT id, dedupe_hash, status, opt_out, created_at
                    FROM companies
                    WHERE dedupe_hash IS NOT NULL AND dedupe_hash <> ''
                    """
                )
            ).mappings()
        )

        groups: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for row in rows:
            dedupe_hash = (row["dedupe_hash"] or "").strip()
            if not dedupe_hash:
                continue
            materialized = {
                "id": str(row["id"]),
                "dedupe_hash": dedupe_hash,
                "status": row["status"],
                "opt_out": row["opt_out"],
                "created_at": row["created_at"],
            }
            groups[dedupe_hash].append(materialized)

        primary_ids: Dict[str, str] = {}
        duplicate_ids: Dict[str, str] = {}

        for dedupe_hash, values in groups.items():
            if len(values) == 1:
                primary_ids[values[0]["id"]] = dedupe_hash
                continue

            sorted_values = sorted(
                values,
                key=lambda item: (item["created_at"], item["id"]),
            )
            primary = sorted_values[0]
            primary_ids[primary["id"]] = dedupe_hash

            for duplicate in sorted_values[1:]:
                duplicate_ids[duplicate["id"]] = dedupe_hash

        return primary_ids, duplicate_ids

    def _apply_duplicate_updates(
        self,
        session: Session,
        primary_ids: Dict[str, str],
        duplicate_ids: Dict[str, str],
    ) -> int:
        updated = 0

        for duplicate_id in duplicate_ids:
            result = session.execute(
                text(
                    """
                    UPDATE companies
                    SET status = 'duplicate',
                        opt_out = TRUE,
                        updated_at = NOW()
                    WHERE id = :id AND status <> 'duplicate'
                    """
                ),
                {"id": duplicate_id},
            )
            updated += result.rowcount or 0

        for primary_id in primary_ids:
            session.execute(
                text(
                    """
                    UPDATE companies
                    SET status = CASE WHEN status = 'duplicate' THEN 'new' ELSE status END,
                        opt_out = FALSE,
                        updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {"id": primary_id},
            )

        if updated:
            LOGGER.info("Отмечено дубликатов: %s", updated)
        return updated
