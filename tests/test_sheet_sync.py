"""Тесты сервиса синхронизации листа."""

from datetime import datetime, timezone

from app.modules.query_generator import QueryGenerator
from app.modules.sheet_sync import (
    QueryInsertResult,
    QueryRepository,
    SheetAdapter,
    SheetRowData,
    SheetStatusUpdate,
    SheetSyncService,
    SyncSummary,
)


class FakeSheetAdapter(SheetAdapter):
    def __init__(self) -> None:
        self._rows = [
            SheetRowData(
                row_index=2,
                values={
                    "niche": "стоматология",
                    "city": "Москва",
                    "country": "Россия",
                    "batch_tag": "batch-1",
                    "search_malls": "yes",
                    "search_agencies": "yes",
                    "status": "",
                },
            ),
            SheetRowData(
                row_index=3,
                values={
                    "city": "",
                    "country": "Россия",
                    "batch_tag": "batch-2",
                    "status": "done",
                },
            ),
        ]
        self.updated: list[SheetStatusUpdate] = []

    def fetch_rows(self):  # type: ignore[override]
        return self._rows

    def update_rows(self, updates):  # type: ignore[override]
        self.updated.extend(updates)


class FakeRepository(QueryRepository):
    def __init__(self) -> None:
        self.inserted_batches = []
        self.logged = []

    def insert_queries(self, queries):  # type: ignore[override]
        self.inserted_batches.append(queries)
        first = queries[0].scheduled_for if queries else None
        last = queries[-1].scheduled_for if queries else None
        return QueryInsertResult(
            attempted=len(queries),
            inserted=len(queries),
            duplicates=0,
            first_scheduled=first,
            last_scheduled=last,
        )

    def log_batch(self, row, result, status, error):  # type: ignore[override]
        self.logged.append((row, result, status, error))


def test_sheet_sync_updates_statuses() -> None:
    adapter = FakeSheetAdapter()
    repository = FakeRepository()
    generator = QueryGenerator(now_func=lambda: datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc))

    service = SheetSyncService(adapter, repository, generator)
    summary = service.sync()

    assert isinstance(summary, SyncSummary)
    assert summary.processed_rows == 1
    assert summary.inserted_queries == len(repository.inserted_batches[0])
    assert len(adapter.updated) == 1

    update = adapter.updated[0]
    assert update.status == "done"
    assert update.generated_count == len(repository.inserted_batches[0])
    assert update.generated_count == 4
    assert update.last_error is None
    assert repository.logged[0][2] == "done"
