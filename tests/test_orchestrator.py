"""Тесты оркестратора."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.orchestrator import PipelineOrchestrator


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
