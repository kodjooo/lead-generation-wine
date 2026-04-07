"""Оркестратор пайплайна лидогенерации."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings
from app.modules.deduplicate import DeduplicationService
from app.modules.enrich_contacts import ContactEnricher
from app.modules.generate_email_gpt import (
    CompanyBrief,
    ContactBrief,
    EmailGenerationError,
    EmailGenerator,
    OfferBrief,
)
from app.modules.send_email import EmailSender
from app.modules.serp_ingest import SerpIngestService
from app.modules.utils.db import get_session_factory, session_scope
from app.modules.utils.iam import (
    IamTokenProvider,
    StaticTokenProvider,
    load_service_account_key_from_file,
    load_service_account_key_from_string,
)
from app.modules.yandex_deferred import (
    DeferredQueryParams,
    InvalidResponseError,
    OperationResponse,
    YandexDeferredClient,
)
from app.modules.sheet_sync import build_service as build_sheet_sync_service

LOGGER = logging.getLogger("app.orchestrator")
MAX_SERP_OPERATION_RETRIES = 3
TERMINAL_SERP_OPERATION_STATUS_CODES = {400, 401, 403, 404}

SELECT_PENDING_QUERIES_SQL = """
SELECT id, query_text, region_code
FROM serp_queries
WHERE status = 'pending'
  AND scheduled_for <= NOW()
ORDER BY scheduled_for ASC
LIMIT :limit;
"""

INSERT_OPERATION_SQL = """
INSERT INTO serp_operations (
    query_id,
    operation_id,
    status,
    requested_at,
    metadata
)
VALUES (
    :query_id,
    :operation_id,
    'created',
    NOW(),
    CAST(:metadata AS JSONB)
)
ON CONFLICT (operation_id) DO NOTHING;
"""

UPDATE_QUERY_STATUS_SQL = """
UPDATE serp_queries
SET status = :status,
    updated_at = NOW()
WHERE id = :query_id;
"""

SELECT_OPEN_OPERATIONS_SQL = """
SELECT id, query_id, operation_id, status, retry_count
FROM serp_operations
WHERE status IN ('created', 'running')
ORDER BY requested_at
LIMIT :limit;
"""

UPDATE_OPERATION_STATUS_SQL = """
UPDATE serp_operations
SET status = :status,
    completed_at = :completed_at,
    retry_count = retry_count + :increment_retry,
    error_payload = CAST(:error_payload AS JSONB),
    metadata = metadata || CAST(:metadata AS JSONB),
    modified_at = NOW()
WHERE id = :operation_id;
"""

SELECT_COMPANIES_WITHOUT_CONTACTS_SQL = """
WITH locked_companies AS (
    SELECT c.id, c.canonical_domain, c.industry, c.created_at
    FROM companies c
    WHERE c.canonical_domain IS NOT NULL
      AND c.status = 'new'
      AND NOT EXISTS (
          SELECT 1
          FROM contacts ct
          WHERE ct.company_id = c.id
      )
    ORDER BY c.created_at
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
)
SELECT id, canonical_domain, industry
FROM locked_companies
ORDER BY created_at;
"""

SELECT_CONTACTS_FOR_OUTREACH_SQL = """
WITH locked_contacts AS (
    SELECT ct.id, ct.first_seen_at
    FROM contacts ct
    JOIN companies c ON c.id = ct.company_id
    WHERE ct.contact_type = 'email'
      AND ct.is_primary = TRUE
      AND c.status <> 'excluded_by_llm'
      AND COALESCE(c.opt_out, FALSE) = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM outreach_messages om
          WHERE om.contact_id = ct.id
            AND om.status IN ('sent', 'scheduled')
      )
    ORDER BY ct.first_seen_at
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
)
SELECT
    ct.id AS contact_id,
    ct.company_id,
    ct.value,
    c.canonical_domain,
    c.industry,
    c.attributes ->> 'homepage_excerpt' AS homepage_excerpt
FROM locked_contacts lc
JOIN contacts ct ON ct.id = lc.id
JOIN companies c ON c.id = ct.company_id
ORDER BY lc.first_seen_at;
"""

SELECT_SERP_QUERY_DETAILS_SQL = """
SELECT query_text, region_code
     , metadata
FROM serp_queries
WHERE id = :query_id;
"""

SELECT_SCHEDULED_OUTREACH_SQL = """
WITH locked AS (
    SELECT om.id
    FROM outreach_messages om
    WHERE om.status = 'scheduled'
      AND (om.scheduled_for IS NULL OR om.scheduled_for <= NOW())
    ORDER BY COALESCE(om.scheduled_for, om.created_at)
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
)
SELECT
    om.id,
    om.company_id,
    om.contact_id,
    om.subject,
    om.body,
    om.metadata,
    ct.value AS contact_value
FROM outreach_messages om
JOIN locked l ON l.id = om.id
LEFT JOIN contacts ct ON ct.id = om.contact_id
ORDER BY COALESCE(om.scheduled_for, om.created_at);
"""


@dataclass
class OrchestratorConfig:
    batch_size: int = 5
    poll_interval_seconds: int = 60
    enable_scheduling: bool = True


class PipelineOrchestrator:
    """Связывает все модули в единую последовательность шагов."""

    def __init__(self, config: OrchestratorConfig | None = None) -> None:
        self.config = config or OrchestratorConfig()
        self.session_factory = get_session_factory()
        settings = get_settings()
        token_provider = self._build_iam_provider(settings)
        self.deferred_client = YandexDeferredClient(
            token_provider=token_provider.get_token,
            folder_id=settings.yandex_folder_id,
            timezone=settings.timezone,
            enforce_night_window=settings.yandex_enforce_night_window,
        )
        self.serp_ingest = SerpIngestService(self.session_factory)
        self.deduplicator = DeduplicationService(self.session_factory)
        self.contact_enricher = ContactEnricher(session_factory=self.session_factory)
        self.email_generator = EmailGenerator()
        self.email_sender = EmailSender(session_factory=self.session_factory)
        self.email_generation_enabled = settings.email_generation_enabled
        self._token_provider = token_provider
        self._results_processing_mode = settings.yandex_results_processing_mode
        self._pipeline_tz = ZoneInfo(settings.timezone)
        self.sheet_settings = settings.sheet_sync
        self._sheet_service = None
        self._sheet_sync_interval = timedelta(minutes=max(1, self.sheet_settings.interval_minutes))
        self._last_sheet_sync: datetime | None = None
        if self.sheet_settings.enabled:
            try:
                self._sheet_service = build_sheet_sync_service(settings)
                LOGGER.info(
                    "Автосинхронизация Google Sheets включена (каждые %s мин, batch_tag=%s)",
                    self.sheet_settings.interval_minutes,
                    self.sheet_settings.batch_tag,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Не удалось инициализировать синхронизацию Google Sheets: %s", exc)
                self._sheet_service = None

    @staticmethod
    def _build_iam_provider(settings) -> StaticTokenProvider | IamTokenProvider:
        if settings.yandex_iam_token:
            return StaticTokenProvider(settings.yandex_iam_token)

        if settings.yandex_sa_key_path:
            key = load_service_account_key_from_file(Path(settings.yandex_sa_key_path))
            return IamTokenProvider(key=key)

        if settings.yandex_sa_key_json:
            key = load_service_account_key_from_string(settings.yandex_sa_key_json)
            return IamTokenProvider(key=key)

        raise RuntimeError(
            "Не настроена авторизация Yandex Cloud: задайте YANDEX_CLOUD_IAM_TOKEN "
            "или путь/JSON ключа сервисного аккаунта."
        )

    def run_once(self) -> None:
        self._maybe_sync_sheet()
        LOGGER.info("Выполнение цикла оркестрации.")
        scheduled = 0
        if self.config.enable_scheduling:
            scheduled = self._schedule_deferred_queries()
        processed = self._poll_operations()
        if processed:
            self.deduplicator.run()
        LOGGER.info(
            "Цикл завершён: scheduled=%s, processed=%s",
            scheduled,
            processed,
        )

    def run_forever(self) -> None:
        LOGGER.info("Запуск оркестратора в режиме цикла (%s c).", self.config.poll_interval_seconds)
        while True:
            self.run_once()
            time.sleep(self.config.poll_interval_seconds)

    def schedule_deferred_queries(self) -> int:
        """Публичный метод для планировщика."""
        if not self.config.enable_scheduling:
            LOGGER.debug("Запрос на постановку операций пропущен: планирование отключено для этого сервиса.")
            return 0
        return self._schedule_deferred_queries()

    def poll_operations(self) -> int:
        """Публичный метод для обновления статусов операций."""
        return self._poll_operations()

    def enrich_missing_contacts(self) -> int:
        """Публичный метод для воркера контактов."""
        return self._enrich_missing_contacts()

    def generate_and_send_emails(self) -> int:
        """Генерация и отправка писем."""
        _, sent = self._generate_and_send_emails()
        return sent

    def run_worker_cycle(self) -> tuple[int, int, int]:
        """Отдельный цикл для worker: enrichment -> queue -> send."""
        enriched = self._enrich_missing_contacts()
        queued, sent = self._generate_and_send_emails()
        return enriched, queued, sent

    def _maybe_sync_sheet(self) -> None:
        if not self._sheet_service:
            return
        now = datetime.now(timezone.utc)
        if self._last_sheet_sync and now - self._last_sheet_sync < self._sheet_sync_interval:
            return
        try:
            summary = self._sheet_service.sync(batch_tag=self.sheet_settings.batch_tag)
            LOGGER.info(
                "Синхронизация Google Sheets: обработано=%s, добавлено=%s, дубликатов=%s, ошибок=%s",
                summary.processed_rows,
                summary.inserted_queries,
                summary.duplicate_queries,
                summary.errors,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Ошибка синхронизации Google Sheets: %s", exc)
        finally:
            self._last_sheet_sync = now

    def _schedule_deferred_queries(self) -> int:
        with session_scope(self.session_factory) as session:
            rows = list(
                session.execute(
                    text(SELECT_PENDING_QUERIES_SQL),
                    {"limit": self.config.batch_size},
                ).mappings()
            )
            if not rows:
                return 0

            scheduled = 0
            for row in rows:
                try:
                    params = DeferredQueryParams(query_text=row["query_text"], region=row["region_code"])
                    operation = self.deferred_client.create_deferred_search(params)
                    session.execute(
                        text(INSERT_OPERATION_SQL),
                        {
                            "query_id": row["id"],
                            "operation_id": operation.id,
                            "metadata": json.dumps({"created_at": datetime.now(timezone.utc).isoformat()}),
                        },
                    )
                    session.execute(
                        text(UPDATE_QUERY_STATUS_SQL),
                        {"query_id": row["id"], "status": "in_progress"},
                    )
                    scheduled += 1
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("Не удалось создать deferred-запрос: %s", exc)
            return scheduled

    def _poll_operations(self) -> int:
        if not self._should_poll_operations_now():
            LOGGER.info(
                "Polling deferred-операций пропущен: YANDEX_RESULTS_PROCESSING_MODE=%s, сейчас вне ночного окна.",
                self._results_processing_mode,
            )
            return 0

        with session_scope(self.session_factory) as session:
            rows = list(
                session.execute(
                    text(SELECT_OPEN_OPERATIONS_SQL),
                    {"limit": self.config.batch_size},
                ).mappings()
            )
            if not rows:
                return 0

            processed = 0
            for row in rows:
                operation_id = row["operation_id"]
                try:
                    operation = self.deferred_client.get_operation(operation_id)
                    status = "running" if not operation.done else "done"
                    metadata = {"last_checked": datetime.now(timezone.utc).isoformat()}

                    if operation.done:
                        self._handle_completed_operation(session, row["id"], row["query_id"], operation)
                        processed += 1
                        status = "done"
                        completed_at = datetime.now(timezone.utc)
                    else:
                        completed_at = None

                    session.execute(
                        text(UPDATE_OPERATION_STATUS_SQL),
                        {
                            "operation_id": row["id"],
                            "status": status,
                            "completed_at": completed_at,
                            "increment_retry": 0,
                            "error_payload": None,
                            "metadata": json.dumps(metadata),
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    next_query_status = self._resolve_query_status_after_operation_error(
                        retry_count=row["retry_count"],
                        error=exc,
                    )
                    LOGGER.exception("Ошибка обработки операции %s: %s", operation_id, exc)
                    session.execute(
                        text(UPDATE_QUERY_STATUS_SQL),
                        {"query_id": row["query_id"], "status": next_query_status},
                    )
                    session.execute(
                        text(UPDATE_OPERATION_STATUS_SQL),
                        {
                            "operation_id": row["id"],
                            "status": "failed",
                            "completed_at": datetime.now(timezone.utc),
                            "increment_retry": 1,
                            "error_payload": json.dumps({"reason": str(exc)}),
                            "metadata": json.dumps({}),
                        },
                    )
            return processed

    @staticmethod
    def _resolve_query_status_after_operation_error(*, retry_count: int, error: Exception) -> str:
        next_retry_count = retry_count + 1
        status_code = getattr(error, "status_code", None)

        if status_code in TERMINAL_SERP_OPERATION_STATUS_CODES:
            return "failed"
        if next_retry_count >= MAX_SERP_OPERATION_RETRIES:
            return "failed"
        return "pending"

    def _should_poll_operations_now(self) -> bool:
        if self._results_processing_mode != "night_only":
            return True
        now_local = datetime.now(timezone.utc).astimezone(self._pipeline_tz)
        return 0 <= now_local.hour < 8

    def _handle_completed_operation(
        self,
        session: Session,
        operation_db_id: str,
        query_id: str,
        operation: OperationResponse,
    ) -> None:
        if operation.error:
            raise InvalidResponseError(
                f"Операция {operation.id} завершилась с ошибкой: {json.dumps(operation.error, ensure_ascii=False)}"
            )

        raw_xml = operation.decode_raw_data()

        query_row = session.execute(
            text(SELECT_SERP_QUERY_DETAILS_SQL),
            {"query_id": query_id},
        ).mappings().one_or_none()
        metadata = query_row["metadata"] if query_row else {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}

        self.serp_ingest.ingest(
            operation_db_id,
            raw_xml,
            yandex_operation_id=operation.id,
            query_metadata=metadata if isinstance(metadata, dict) else {},
        )
        session.execute(
            text(UPDATE_QUERY_STATUS_SQL),
            {"query_id": query_id, "status": "completed"},
        )

    def _enrich_missing_contacts(self) -> int:
        with session_scope(self.session_factory) as session:
            rows = list(
                session.execute(
                    text(SELECT_COMPANIES_WITHOUT_CONTACTS_SQL),
                    {"limit": self.config.batch_size},
                ).mappings()
            )
            count = 0
            for row in rows:
                canonical_domain = (row["canonical_domain"] or "").strip()
                if not canonical_domain:
                    LOGGER.debug("У компании %s отсутствует canonical_domain, пропускаем обогащение.", row["id"])
                    continue
                inserted = self.contact_enricher.enrich_company(
                    company_id=str(row["id"]),
                    canonical_domain=canonical_domain,
                    industry=row.get("industry"),
                    session=session,
                )
                if inserted:
                    count += 1
            return count

    def _generate_and_send_emails(self) -> tuple[int, int]:
        queued = self._queue_emails()
        sent = self._send_scheduled_emails()
        return queued, sent

    def _queue_emails(self) -> int:
        if not self.email_generation_enabled:
            LOGGER.info("Генерация писем отключена настройкой EMAIL_GENERATION_ENABLED.")
            return 0
        with session_scope(self.session_factory) as session:
            rows = list(
                session.execute(
                    text(SELECT_CONTACTS_FOR_OUTREACH_SQL),
                    {"limit": self.config.batch_size},
                ).mappings()
            )
            queued = 0
            for row in rows:
                company = CompanyBrief(
                    name=None,
                    domain=row["canonical_domain"] or row["value"].split("@")[-1],
                    entity_type=row["industry"],
                    industry=row["industry"],
                    highlights=[row["homepage_excerpt"]] if row["homepage_excerpt"] else [],
                )
                contact = ContactBrief(emails=[row["value"]])
                try:
                    generated = self.email_generator.generate(
                        company,
                        self._build_offer(company.entity_type),
                        contact,
                    )
                except EmailGenerationError as exc:
                    LOGGER.error(
                        "Не удалось сгенерировать письмо для company_id=%s contact_id=%s: %s",
                        row["company_id"],
                        row["contact_id"],
                        exc,
                    )
                    continue
                self.email_sender.queue(
                    company_id=row["company_id"],
                    contact_id=row["contact_id"],
                    to_email=row["value"],
                    template=generated.template,
                    request_payload=generated.request_payload,
                    session=session,
                )
                queued += 1
            return queued

    @staticmethod
    def _build_offer(entity_type: str | None) -> OfferBrief:
        if entity_type == "mall":
            return OfferBrief(
                pains=[
                    "Нужна локация с подходящим трафиком и условиями аренды",
                    "Нужно быстро получить коммерческие условия и информацию по площадям",
                ],
                value_proposition=(
                    "Рассматриваем размещение магазина в торговом центре и готовы изучить условия аренды"
                ),
                call_to_action="Если у вас есть релевантные площади, буду признателен за коммерческое предложение.",
            )
        if entity_type == "real_estate_agency":
            return OfferBrief(
                pains=[
                    "Нужно быстро получить релевантные помещения под формат магазина",
                    "Важно отсечь нерелевантные объекты и быстро собрать коммерческие условия",
                ],
                value_proposition=(
                    "Ищем помещение под магазин в вашем городе и готовы рассмотреть подходящие объекты"
                ),
                call_to_action="Если можете предложить подходящие объекты, буду признателен за подборку или коммерческое предложение.",
            )
        return OfferBrief(
            pains=["Ищем подходящее помещение для аренды"],
            value_proposition="Рассматриваем объекты в городе для размещения магазина.",
            call_to_action="Если тема актуальна, буду признателен за информацию по доступным вариантам.",
        )

    def _send_scheduled_emails(self) -> int:
        if not getattr(self.email_sender, "sending_enabled", True):
            LOGGER.debug("Отправка писем отключена, доставка пропущена.")
            return 0
        if not self.email_sender.is_within_send_window():
            LOGGER.debug("Вне окна отправки, доставка писем пропущена.")
            return 0

        with session_scope(self.session_factory) as session:
            rows = list(
                session.execute(
                    text(SELECT_SCHEDULED_OUTREACH_SQL),
                    {"limit": self.config.batch_size},
                ).mappings()
            )
            sent = 0
            for row in rows:
                metadata = row.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        metadata = {}
                to_email = None
                if isinstance(metadata, dict):
                    to_email = metadata.get("to_email")
                if not to_email:
                    to_email = row.get("contact_value")
                if not to_email:
                    LOGGER.error(
                        "Не удалось определить email для outreach %s, помечаем как failed.",
                        row["id"],
                    )
                    self.email_sender.mark_status(
                        outreach_id=str(row["id"]),
                        status="failed",
                        last_error="missing_email",
                        metadata={"reason": "missing_email"},
                        session=session,
                    )
                    continue
                result = self.email_sender.deliver(
                    outreach_id=str(row["id"]),
                    company_id=str(row["company_id"]),
                    contact_id=row.get("contact_id"),
                    to_email=to_email,
                    subject=row["subject"],
                    body=row["body"],
                    session=session,
                )
                if result == "sent":
                    sent += 1
            return sent
