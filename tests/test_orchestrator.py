"""Тесты оркестратора."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from app.modules.yandex_deferred import YandexAPIError
from app.modules.generate_email_gpt import EmailGenerationError
from app.orchestrator import (
    PipelineOrchestrator,
    SELECT_COMPANIES_WITHOUT_CONTACTS_SQL,
    SELECT_CONTACTS_FOR_OUTREACH_SQL,
)
from app.tools.cleanup_llm_irrelevant_sites import IRRELEVANT_VERDICTS, _build_company_patch


def test_should_poll_operations_now_allows_anytime(monkeypatch) -> None:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._results_processing_mode = "anytime"
    orchestrator._pipeline_tz = ZoneInfo("Europe/Moscow")

    assert orchestrator._should_poll_operations_now() is True


def test_should_poll_operations_now_blocks_daytime_for_night_only(monkeypatch) -> None:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._results_processing_mode = "night_only"
    orchestrator._pipeline_tz = ZoneInfo("Europe/Moscow")

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc)
            if tz is not None:
                return value.astimezone(tz)
            return value

    monkeypatch.setattr("app.orchestrator.datetime", FixedDatetime)

    assert orchestrator._should_poll_operations_now() is False


def test_queue_emails_returns_zero_when_generation_disabled() -> None:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.email_generation_enabled = False

    assert orchestrator._queue_emails() == 0


def test_run_once_handles_orchestration_only() -> None:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.config = SimpleNamespace(enable_scheduling=True)
    orchestrator._maybe_sync_sheet = Mock()
    orchestrator._schedule_deferred_queries = Mock(return_value=2)
    orchestrator._poll_operations = Mock(return_value=1)
    orchestrator._enrich_missing_contacts = Mock()
    orchestrator._generate_and_send_emails = Mock()
    orchestrator.deduplicator = SimpleNamespace(run=Mock())

    orchestrator.run_once()

    orchestrator._maybe_sync_sheet.assert_called_once_with()
    orchestrator._schedule_deferred_queries.assert_called_once_with()
    orchestrator._poll_operations.assert_called_once_with()
    orchestrator.deduplicator.run.assert_called_once_with()
    orchestrator._enrich_missing_contacts.assert_not_called()
    orchestrator._generate_and_send_emails.assert_not_called()


def test_run_worker_cycle_runs_enrichment_and_email_generation() -> None:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._enrich_missing_contacts = Mock(return_value=3)
    orchestrator._generate_and_send_emails = Mock(return_value=(2, 1))

    assert orchestrator.run_worker_cycle() == (3, 2, 1)


def test_queue_emails_skips_contacts_when_generation_fails(monkeypatch) -> None:
    class _FakeMappingsResultWithRows:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return iter(self._rows)

    class _QueueSession:
        def execute(self, sql, params=None):
            sql_text = str(sql)
            if "FROM locked_contacts" in sql_text:
                return _FakeMappingsResultWithRows(
                    [
                        {
                            "contact_id": "contact-1",
                            "company_id": "company-1",
                            "value": "hello@example.com",
                            "canonical_domain": "example.com",
                            "industry": "mall",
                            "homepage_excerpt": "торговый центр",
                        }
                    ]
                )
            return _FakeMappingsResultWithRows([])

    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.email_generation_enabled = True
    orchestrator.config = SimpleNamespace(batch_size=5)
    orchestrator.session_factory = object()
    orchestrator.email_generator = SimpleNamespace(
        generate=Mock(side_effect=EmailGenerationError("llm unavailable"))
    )
    orchestrator.email_sender = SimpleNamespace(queue=Mock())

    @contextmanager
    def fake_scope(_):
        yield _QueueSession()

    monkeypatch.setattr("app.orchestrator.session_scope", fake_scope)

    assert orchestrator._queue_emails() == 0
    orchestrator.email_sender.queue.assert_not_called()


def test_worker_enrichment_queue_prioritizes_only_new_companies() -> None:
    assert "c.status = 'new'" in SELECT_COMPANIES_WITHOUT_CONTACTS_SQL
    assert "c.status <> 'contacts_not_found'" not in SELECT_COMPANIES_WITHOUT_CONTACTS_SQL


def test_outreach_queue_skips_llm_excluded_and_opt_out_companies() -> None:
    assert "c.status <> 'excluded_by_llm'" in SELECT_CONTACTS_FOR_OUTREACH_SQL
    assert "COALESCE(c.opt_out, FALSE) = FALSE" in SELECT_CONTACTS_FOR_OUTREACH_SQL
    assert "AND c.attributes ->> 'homepage_excerpt' IS NOT NULL" not in SELECT_CONTACTS_FOR_OUTREACH_SQL
    assert "AND c.attributes ->> 'homepage_excerpt' <> ''" not in SELECT_CONTACTS_FOR_OUTREACH_SQL
    assert "FROM opt_out_registry" not in SELECT_CONTACTS_FOR_OUTREACH_SQL


def test_cleanup_llm_company_patch_marks_company_as_excluded() -> None:
    candidate = SimpleNamespace(
        llm_site_verdict="aggregator_or_directory",
        llm_reason="Это каталог агентств, а не официальный сайт.",
    )

    payload = _build_company_patch(candidate)

    assert '"excluded_by_llm": true' in payload
    assert '"excluded_reason": "irrelevant_llm_site_verdict"' in payload
    assert any(verdict in payload for verdict in IRRELEVANT_VERDICTS)


class _FakeMappingsResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return iter(self._rows)


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        sql_text = str(sql)
        self.calls.append((sql_text, params))
        if "FROM serp_operations" in sql_text and "WHERE status IN ('created', 'running')" in sql_text:
            return _FakeMappingsResult(self.rows)
        return _FakeMappingsResult([])


@contextmanager
def _fake_session_scope(session):
    yield session


def test_poll_operations_marks_query_failed_on_404(monkeypatch) -> None:
    fake_session = _FakeSession(
        [
            {
                "id": "op-db-id",
                "query_id": "query-id",
                "operation_id": "yandex-op",
                "status": "created",
                "retry_count": 0,
            }
        ]
    )
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.config = SimpleNamespace(batch_size=5)
    orchestrator.session_factory = object()
    orchestrator.deferred_client = SimpleNamespace(
        get_operation=Mock(side_effect=YandexAPIError("Ошибка получения операции: 404", status_code=404))
    )
    orchestrator._should_poll_operations_now = Mock(return_value=True)

    monkeypatch.setattr("app.orchestrator.session_scope", lambda _: _fake_session_scope(fake_session))

    assert orchestrator._poll_operations() == 0

    query_updates = [params for _, params in fake_session.calls if params and params.get("query_id") == "query-id"]
    operation_updates = [params for _, params in fake_session.calls if params and params.get("operation_id") == "op-db-id"]

    assert query_updates[-1]["status"] == "failed"
    assert operation_updates[-1]["status"] == "failed"
    assert operation_updates[-1]["increment_retry"] == 1


def test_poll_operations_requeues_query_on_retryable_error(monkeypatch) -> None:
    fake_session = _FakeSession(
        [
            {
                "id": "op-db-id",
                "query_id": "query-id",
                "operation_id": "yandex-op",
                "status": "running",
                "retry_count": 1,
            }
        ]
    )
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.config = SimpleNamespace(batch_size=5)
    orchestrator.session_factory = object()
    orchestrator.deferred_client = SimpleNamespace(
        get_operation=Mock(side_effect=YandexAPIError("Ошибка получения операции: 503", status_code=503))
    )
    orchestrator._should_poll_operations_now = Mock(return_value=True)

    monkeypatch.setattr("app.orchestrator.session_scope", lambda _: _fake_session_scope(fake_session))

    assert orchestrator._poll_operations() == 0

    query_updates = [params for _, params in fake_session.calls if params and params.get("query_id") == "query-id"]
    operation_updates = [params for _, params in fake_session.calls if params and params.get("operation_id") == "op-db-id"]

    assert query_updates[-1]["status"] == "pending"
    assert operation_updates[-1]["status"] == "failed"
    assert operation_updates[-1]["increment_retry"] == 1
