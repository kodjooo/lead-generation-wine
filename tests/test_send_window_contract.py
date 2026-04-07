from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.modules.send_email import EmailSender


class DummySession:
    def execute(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("DB should not be used in this test")


def reset_settings_cache() -> None:
    from app.config import get_settings

    get_settings.cache_clear()


def test_email_sender_is_within_send_window_uses_local_timezone(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_settings_cache()
    monkeypatch.setenv("TIMEZONE", "Europe/Moscow")

    sender = EmailSender(session_factory=lambda: DummySession(), use_starttls=False)  # type: ignore[arg-type]

    assert sender.is_within_send_window(datetime(2026, 4, 7, 9, 30, tzinfo=timezone.utc)) is True
    assert sender.is_within_send_window(datetime(2026, 4, 7, 6, 0, tzinfo=timezone.utc)) is False

    monkeypatch.delenv("TIMEZONE", raising=False)
    reset_settings_cache()
