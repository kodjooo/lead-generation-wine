"""Тесты сервиса дедупликации компаний."""

from datetime import datetime, timezone
from typing import Any, Dict

from app.modules.deduplicate import DeduplicationService


class DummyMappingResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self._rows


class DummyUpdateResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class DummySession:
    def __init__(self) -> None:
        base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.company_rows: Dict[str, Dict[str, Any]] = {
            "1": {
                "id": "1",
                "canonical_domain": "alpha.ru",
                "website_url": "https://alpha.ru",
                "dedupe_hash": "",
                "status": "new",
                "opt_out": False,
                "created_at": base_time,
            },
            "2": {
                "id": "2",
                "canonical_domain": None,
                "website_url": "alpha.ru",
                "dedupe_hash": "",
                "status": "new",
                "opt_out": False,
                "created_at": base_time.replace(hour=1),
            },
            "3": {
                "id": "3",
                "canonical_domain": "beta.ru",
                "website_url": "https://beta.ru",
                "dedupe_hash": "",
                "status": "new",
                "opt_out": False,
                "created_at": base_time.replace(hour=2),
            },
        }
        self.executed = []

    def execute(self, statement, params=None):  # noqa: D401, ANN001
        sql = statement.text if hasattr(statement, "text") else str(statement)
        params = params or {}
        self.executed.append((sql.strip(), params))

        if "SELECT id, canonical_domain" in sql:
            rows = [
                {
                    "id": row["id"],
                    "canonical_domain": row["canonical_domain"],
                    "website_url": row["website_url"],
                    "dedupe_hash": row["dedupe_hash"],
                }
                for row in self.company_rows.values()
            ]
            return DummyMappingResult(rows)

        if "SELECT id, dedupe_hash" in sql:
            rows = [
                {
                    "id": row["id"],
                    "dedupe_hash": row["dedupe_hash"],
                    "status": row["status"],
                    "opt_out": row["opt_out"],
                    "created_at": row["created_at"],
                }
                for row in self.company_rows.values()
            ]
            return DummyMappingResult(rows)

        if "SET dedupe_hash" in sql:
            company = self.company_rows[params["id"]]
            company["dedupe_hash"] = params["dedupe_hash"]
            company["canonical_domain"] = params["canonical_domain"]
            return DummyUpdateResult(1)

        if "SET status = 'duplicate'" in sql:
            company = self.company_rows[params["id"]]
            if company["status"] == "duplicate":
                return DummyUpdateResult(0)
            company["status"] = "duplicate"
            company["opt_out"] = True
            return DummyUpdateResult(1)

        if "SET status = CASE WHEN status = 'duplicate' THEN 'new'" in sql:
            company = self.company_rows[params["id"]]
            if company["status"] == "duplicate":
                company["status"] = "new"
            company["opt_out"] = False
            return DummyUpdateResult(1)

        raise AssertionError(f"Unexpected SQL executed: {sql}")

    def commit(self) -> None:  # noqa: D401
        pass

    def rollback(self) -> None:  # noqa: D401
        pass

    def close(self) -> None:  # noqa: D401
        pass


def test_deduplication_marks_duplicates() -> None:
    session = DummySession()
    service = DeduplicationService(session_factory=lambda: session)  # type: ignore[arg-type]

    stats = service.run(session=session)

    assert stats.hash_updates == 3
    assert stats.primary_companies == 2
    assert stats.duplicates_marked == 1
    assert stats.updated_records == 1

    assert session.company_rows["1"]["status"] == "new"
    assert session.company_rows["1"]["opt_out"] is False
    assert session.company_rows["2"]["status"] == "duplicate"
    assert session.company_rows["2"]["opt_out"] is True
    assert session.company_rows["3"]["status"] == "new"
