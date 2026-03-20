"""Синхронизация Google Sheets -> очередь запросов по городам."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Protocol

import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import text

from app.modules.query_generator import CityRow, GeneratedQuery, QueryGenerator
from app.modules.utils.db import get_session_factory, session_scope

LOGGER = logging.getLogger("app.sheet_sync")

STATUS_COLUMNS = [
    "status",
    "generated_count",
    "db_inserted_count",
    "db_duplicate_count",
    "db_first_scheduled_for",
    "db_last_scheduled_for",
    "last_error",
]


@dataclass
class SheetRowData:
    """Данные строки листа."""

    row_index: int
    values: dict[str, str]

    def get(self, key: str) -> str:
        return self.values.get(key.lower(), "").strip()


@dataclass
class SheetStatusUpdate:
    row_index: int
    status: str
    generated_count: int
    inserted_count: int
    duplicate_count: int
    first_scheduled: Optional[datetime]
    last_scheduled: Optional[datetime]
    last_error: Optional[str]


@dataclass
class QueryInsertResult:
    attempted: int
    inserted: int
    duplicates: int
    first_scheduled: Optional[datetime]
    last_scheduled: Optional[datetime]


@dataclass
class SyncSummary:
    total_rows: int = 0
    processed_rows: int = 0
    inserted_queries: int = 0
    duplicate_queries: int = 0
    errors: int = 0


class SheetAdapter(Protocol):
    """Интерфейс доступа к листу."""

    def fetch_rows(self) -> List[SheetRowData]:
        ...

    def update_rows(self, updates: List[SheetStatusUpdate]) -> None:
        ...


class GoogleSheetAdapter:
    """Адаптер для работы с Google Sheets."""

    def __init__(
        self,
        *,
        sheet_id: str,
        tab_name: str,
        service_account_file: Optional[str],
        service_account_json: Optional[str],
    ) -> None:
        credentials = self._build_credentials(service_account_file, service_account_json)
        client = gspread.authorize(credentials)
        self._worksheet = client.open_by_key(sheet_id).worksheet(tab_name)
        self._tab_name = tab_name
        self._header_map: dict[str, int] | None = None

    @staticmethod
    def _build_credentials(path: Optional[str], raw_json: Optional[str]) -> Credentials:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        if path:
            return Credentials.from_service_account_file(path, scopes=scopes)
        if raw_json:
            data = json.loads(raw_json)
            return Credentials.from_service_account_info(data, scopes=scopes)
        raise RuntimeError("Не заданы GOOGLE_SA_KEY_FILE или GOOGLE_SA_KEY_JSON для доступа к Sheets.")

    @staticmethod
    def _normalize_header(header: str) -> str:
        return header.strip().lower()

    @staticmethod
    def _column_letter(index: int) -> str:
        result = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def fetch_rows(self) -> List[SheetRowData]:
        raw = self._worksheet.get_all_values()
        if not raw:
            return []
        headers = raw[0]
        header_map = {self._normalize_header(h): idx + 1 for idx, h in enumerate(headers)}
        self._header_map = header_map
        rows: List[SheetRowData] = []
        for row_idx, values in enumerate(raw[1:], start=2):
            row_dict = {
                self._normalize_header(headers[col_idx]): (values[col_idx].strip() if col_idx < len(values) else "")
                for col_idx in range(len(headers))
            }
            rows.append(SheetRowData(row_index=row_idx, values=row_dict))
        return rows

    def update_rows(self, updates: List[SheetStatusUpdate]) -> None:
        if not updates:
            return
        if not self._header_map:
            raise RuntimeError("Не удалось определить заголовки листа перед обновлением.")
        missing = [col for col in STATUS_COLUMNS if col not in self._header_map]
        if missing:
            raise RuntimeError(f"В листе отсутствуют необходимые столбцы: {', '.join(missing)}")

        requests = []
        for update in updates:
            values = [
                update.status,
                str(update.generated_count),
                str(update.inserted_count),
                str(update.duplicate_count),
                update.first_scheduled.isoformat() if update.first_scheduled else "",
                update.last_scheduled.isoformat() if update.last_scheduled else "",
                (update.last_error or ""),
            ]
            start_col = self._header_map[STATUS_COLUMNS[0]]
            end_col = self._header_map[STATUS_COLUMNS[-1]]
            range_a1 = (
                f"{self._column_letter(start_col)}{update.row_index}:"
                f"{self._column_letter(end_col)}{update.row_index}"
            )
            requests.append({"range": range_a1, "values": [values]})
        if requests:
            self._worksheet.batch_update(requests)


class QueryRepository:
    """Хранилище запросов в БД."""

    def __init__(self, session_factory=None) -> None:
        self._session_factory = session_factory or get_session_factory()

    def insert_queries(self, queries: List[GeneratedQuery]) -> QueryInsertResult:
        attempted = len(queries)
        inserted = 0
        duplicates = 0
        first_scheduled: Optional[datetime] = None
        last_scheduled: Optional[datetime] = None
        if not attempted:
            return QueryInsertResult(attempted, 0, 0, None, None)

        with session_scope(self._session_factory) as session:
            for query in queries:
                stmt = text(
                    """
                    INSERT INTO serp_queries (query_text, query_hash, region_code, is_night_window, status, scheduled_for, metadata)
                    VALUES (:query_text, :query_hash, :region_code, TRUE, 'pending', :scheduled_for, CAST(:metadata AS JSONB))
                    ON CONFLICT (query_hash) DO NOTHING
                    RETURNING id
                    """
                )
                params = {
                    "query_text": query.query_text,
                    "query_hash": query.query_hash,
                    "region_code": query.region_code,
                    "scheduled_for": query.scheduled_for,
                    "metadata": json.dumps(query.metadata, ensure_ascii=False),
                }
                result = session.execute(stmt, params)
                inserted_id = result.scalar_one_or_none()
                if inserted_id is not None:
                    inserted += 1
                    if first_scheduled is None or query.scheduled_for < first_scheduled:
                        first_scheduled = query.scheduled_for
                    if last_scheduled is None or query.scheduled_for > last_scheduled:
                        last_scheduled = query.scheduled_for
                else:
                    duplicates += 1
        return QueryInsertResult(attempted, inserted, duplicates, first_scheduled, last_scheduled)

    def log_batch(
        self,
        row: CityRow,
        result: QueryInsertResult,
        status: str,
        error: Optional[str],
    ) -> None:
        with session_scope(self._session_factory) as session:
            stmt = text(
                """
                INSERT INTO search_batch_logs (
                    city, country, batch_tag, entity_scope,
                    attempted_queries, inserted_queries, duplicate_queries,
                    scheduled_start, scheduled_end,
                    status, error
                )
                VALUES (
                    :city, :country, :batch_tag, :entity_scope,
                    :attempted, :inserted, :duplicates,
                    :first_scheduled, :last_scheduled,
                    :status, :error
                )
                """
            )
            params = {
                "city": row.city.strip() if row.city else None,
                "country": row.country.strip() if row.country else None,
                "batch_tag": row.batch_tag.strip() if row.batch_tag else None,
                "entity_scope": self._build_entity_scope(row),
                "attempted": result.attempted,
                "inserted": result.inserted,
                "duplicates": result.duplicates,
                "first_scheduled": result.first_scheduled,
                "last_scheduled": result.last_scheduled,
                "status": status,
                "error": (error[:500] if error else None),
            }
            session.execute(stmt, params)

    @staticmethod
    def _build_entity_scope(row: CityRow) -> str:
        scopes = []
        if row.enabled_malls:
            scopes.append("mall")
        if row.enabled_agencies:
            scopes.append("real_estate_agency")
        return ",".join(scopes) or "none"


class SheetSyncService:
    """Основной сценарий синхронизации листа с очередью."""

    def __init__(
        self,
        sheet_adapter: SheetAdapter,
        query_repository: QueryRepository,
        generator: QueryGenerator,
    ) -> None:
        self.sheet_adapter = sheet_adapter
        self.repository = query_repository
        self.generator = generator

    def sync(self, *, batch_tag: Optional[str] = None) -> SyncSummary:
        rows = self.sheet_adapter.fetch_rows()
        updates: List[SheetStatusUpdate] = []
        summary = SyncSummary(total_rows=len(rows))

        for row_data in rows:
            city = row_data.get("city")
            if not city:
                continue
            if batch_tag and row_data.get("batch_tag") != batch_tag:
                continue

            current_status = row_data.get("status").lower()
            if current_status == "done":
                continue

            summary.processed_rows += 1
            row = CityRow(
                row_index=row_data.row_index,
                city=city,
                country=row_data.get("country") or None,
                batch_tag=row_data.get("batch_tag") or None,
                enabled_malls=self._parse_bool_cell(row_data.get("search_malls"), default=True),
                enabled_agencies=self._parse_bool_cell(row_data.get("search_agencies"), default=True),
            )

            queries: List[GeneratedQuery] = []
            error_message: Optional[str] = None
            status_value = "done"
            try:
                queries = self.generator.generate(row)
                result = self.repository.insert_queries(queries)
                summary.inserted_queries += result.inserted
                summary.duplicate_queries += result.duplicates
                if result.attempted == 0:
                    status_value = "skipped"
                self.repository.log_batch(row, result, status_value, None)
            except Exception as exc:  # noqa: BLE001
                error_message = str(exc)
                summary.errors += 1
                status_value = "error"
                result = QueryInsertResult(
                    attempted=len(queries),
                    inserted=0,
                    duplicates=len(queries),
                    first_scheduled=None,
                    last_scheduled=None,
                )
                LOGGER.exception("Ошибка обработки строки %s: %s", row.row_index, error_message)
                self.repository.log_batch(row, result, status_value, error_message)

            updates.append(
                SheetStatusUpdate(
                    row_index=row.row_index,
                    status=status_value,
                    generated_count=len(queries),
                    inserted_count=result.inserted,
                    duplicate_count=result.duplicates,
                    first_scheduled=result.first_scheduled,
                    last_scheduled=result.last_scheduled,
                    last_error=error_message,
                )
            )

        if updates:
            self.sheet_adapter.update_rows(updates)
        return summary

    @staticmethod
    def _parse_bool_cell(value: str, *, default: bool) -> bool:
        cleaned = (value or "").strip().lower()
        if not cleaned:
            return default
        return cleaned in {"1", "true", "yes", "y", "да", "x"}


def build_service(settings) -> SheetSyncService:
    """Фабрика сервиса синхронизации на основе конфигурации."""
    adapter = GoogleSheetAdapter(
        sheet_id=settings.google_sheets.sheet_id,
        tab_name=settings.google_sheets.tab_name,
        service_account_file=settings.google_sheets.service_account_key_path,
        service_account_json=settings.google_sheets.service_account_key_json,
    )
    repository = QueryRepository()
    generator = QueryGenerator()
    return SheetSyncService(adapter, repository, generator)
