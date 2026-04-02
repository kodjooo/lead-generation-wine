"""Тесты генерации и отправки писем."""

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from app.config import get_settings
from app.modules.generate_email_gpt import CompanyBrief, EmailGenerator, OfferBrief
from app.modules.send_email import EmailSender
from app.modules.mx_router import MXResult


class DummySelectResult:
    def __init__(self, rows: List[Any]) -> None:
        self._rows = rows

    def first(self) -> Any:
        return self._rows[0] if self._rows else None


class DummyInsertResult:
    def __init__(self, value: str) -> None:
        self._value = value

    def scalar_one(self) -> str:
        return self._value


class DummyUpdateResult:
    def __init__(self, value: str) -> None:
        self._value = value

    def scalar_one(self) -> str:
        return self._value


class DummyScalarResult:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class DummySession:
    def __init__(self, opt_out_emails: Optional[List[str]] = None) -> None:
        self.opt_out_emails = {email.lower() for email in (opt_out_emails or [])}
        self.calls: List[Tuple[str, Dict[str, Any]]] = []

    def execute(self, statement, params=None):  # noqa: ANN001
        sql = statement.text if hasattr(statement, "text") else str(statement)
        params = params or {}
        self.calls.append((sql.strip(), params))

        if "SELECT scheduled_for" in sql and "FROM outreach_messages" in sql:
            last = None
            for recorded_sql, recorded_params in reversed(self.calls[:-1]):
                if "INSERT INTO outreach_messages" in recorded_sql:
                    last = recorded_params.get("scheduled_for")
                    if last is not None:
                        break
            return DummyScalarResult(last)

        if "FROM opt_out_registry" in sql:
            email = params.get("contact_value", "").lower()
            rows = [(1,)] if email in self.opt_out_emails else []
            return DummySelectResult(rows)

        if "INSERT INTO outreach_messages" in sql:
            idx = len([c for c in self.calls if "INSERT INTO outreach_messages" in c[0]])
            return DummyInsertResult(f"outreach-{idx}")

        if "UPDATE outreach_messages" in sql:
            return DummyUpdateResult(params.get("id", "outreach-update"))

        raise AssertionError(f"Unexpected SQL: {sql}")

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def reset_settings_cache() -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]


def test_email_generator_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_settings_cache()
    monkeypatch.setenv("EMAIL_GENERATION_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.delenv("EMAIL_GENERATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("EMAIL_GENERATION_LLM_GATEWAY_API_KEY", raising=False)

    generator = EmailGenerator()
    company = CompanyBrief(name="Test", domain="test.ru", entity_type="mall")
    offer = OfferBrief(
        pains=["Нужна локация с подходящим трафиком"],
        value_proposition="Рассматриваем размещение магазина в торговом центре",
    )

    generated = generator.generate(company, offer)

    assert generated.used_fallback is True
    assert generated.request_payload is None
    assert "розничную сеть по продаже алкогольной продукции" in generated.template.body
    assert "подходящие площади для размещения" in generated.template.body
    assert generated.request_payload is None

    reset_settings_cache()


@respx.mock
def test_email_generator_calls_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_settings_cache()
    monkeypatch.setenv("EMAIL_GENERATION_LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("EMAIL_GENERATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("EMAIL_GENERATION_LLM_GATEWAY_API_KEY", raising=False)

    response_json = {
        "object": "response",
        "output_text": json.dumps({"subject": "Тема", "body": "Текст"}),
    }
    respx.post("https://api.openai.com/v1/responses").mock(
        return_value=httpx.Response(200, json=response_json)
    )

    generator = EmailGenerator()
    company = CompanyBrief(name="Alpha", domain="alpha.ru", entity_type="real_estate_agency", industry="Маркетинг")
    offer = OfferBrief(pains=["Нужны лиды"], value_proposition="Запустим кампанию за 7 дней")

    generated = generator.generate(company, offer)

    assert generated.template.subject == "Тема"
    assert generated.template.body == "Текст"
    assert generated.request_payload is not None
    assert generated.used_fallback is False
    payload_text = generated.request_payload["input"][0]["content"][0]["text"]
    assert "крупной розничной сети по продаже алкогольной продукции" in payload_text
    assert "получить в ответ коммерческое предложение" in payload_text

    reset_settings_cache()


@respx.mock
def test_email_generator_uses_gateway_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_settings_cache()
    monkeypatch.setenv("EMAIL_GENERATION_LLM_PROVIDER", "gateway")
    monkeypatch.setenv("EMAIL_GENERATION_LLM_MODEL", "gpt-5")
    monkeypatch.setenv("EMAIL_GENERATION_LLM_REASONING_EFFORT", "low")
    monkeypatch.setenv("EMAIL_GENERATION_LLM_GATEWAY_URL", "https://llm-gateway.example.com")
    monkeypatch.setenv("EMAIL_GENERATION_LLM_GATEWAY_API_KEY", "gateway-secret")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    route = respx.post("https://llm-gateway.example.com/v1/openai/responses").mock(
        return_value=httpx.Response(
            200,
            json={
                "object": "response",
                "output_text": json.dumps({"subject": "Тема gateway", "body": "Текст gateway"}),
            },
        )
    )

    generator = EmailGenerator()
    company = CompanyBrief(name="Mall", domain="mall.ru", entity_type="mall", industry="mall")
    offer = OfferBrief(value_proposition="Рассматриваем размещение магазина в торговом центре")

    generated = generator.generate(company, offer)

    assert generated.used_fallback is False
    assert generated.template.subject == "Тема gateway"
    assert generated.template.body == "Текст gateway"
    assert route.called is True
    assert route.calls[0].request.headers["Authorization"] == "Bearer gateway-secret"

    request_payload = json.loads(route.calls[0].request.content.decode("utf-8"))
    assert request_payload["model"] == "gpt-5"
    assert request_payload["reasoning"]["effort"] == "low"
    assert request_payload["input"][0]["content"][0]["type"] == "input_text"

    reset_settings_cache()


def test_email_sender_queue_persists_email(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    reset_settings_cache()

    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    template = generator_template()
    monkeypatch.setattr(
        sender,
        "_compute_scheduled_for",
        lambda session, reference=None: datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc),
    )

    outreach_id = sender.queue(
        company_id="c1",
        contact_id="contact1",
        to_email="hello@example.com",
        template=template,
        request_payload={"messages": []},
        session=session,
    )

    assert outreach_id == "outreach-1"
    sql, params = session.calls[-1]
    assert "INSERT INTO outreach_messages" in sql
    assert params["status"] == "scheduled"
    metadata = json.loads(params["metadata"])
    assert metadata["to_email"] == "hello@example.com"
    assert metadata["llm_request"] == {"messages": []}

    reset_settings_cache()


def test_email_sender_queue_spacing(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    reset_settings_cache()


def test_email_sender_queue_skips_invalid_email(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    reset_settings_cache()

    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    template = generator_template()

    outreach_id = sender.queue(
        company_id="c1",
        contact_id="contact1",
        to_email="+74951234567",
        template=template,
        request_payload=None,
        session=session,
    )

    assert outreach_id == "outreach-1"
    sql, params = session.calls[-1]
    assert "INSERT INTO outreach_messages" in sql
    assert params["status"] == "skipped"
    assert params["last_error"] == "invalid_email"
    metadata = json.loads(params["metadata"])
    assert metadata["reason"] == "invalid_email"
    assert metadata["to_email_raw"] == "+74951234567"

    reset_settings_cache()

    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    template = generator_template()

    delays = iter([240, 300])
    monkeypatch.setattr("app.modules.send_email.random.randint", lambda a, b: next(delays))

    class FixedDatetime(datetime):
        _values = iter([])

        @classmethod
        def now(cls, tz=None):
            value = next(cls._values)
            if tz is not None:
                return value.astimezone(tz)
            return value

    FixedDatetime._values = iter(
        [
            datetime(2025, 10, 24, 6, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 10, 24, 6, 0, 5, tzinfo=timezone.utc),
        ]
    )
    monkeypatch.setattr("app.modules.send_email.datetime", FixedDatetime)

    sender.queue(
        company_id="c1",
        contact_id="contact1",
        to_email="first@example.com",
        template=template,
        request_payload=None,
        session=session,
    )
    sender.queue(
        company_id="c2",
        contact_id="contact2",
        to_email="second@example.com",
        template=template,
        request_payload=None,
        session=session,
    )

    scheduled = [
        params["scheduled_for"]
        for sql, params in session.calls
        if "INSERT INTO outreach_messages" in sql
    ]
    assert len(scheduled) >= 2
    diff_seconds = (scheduled[-1] - scheduled[-2]).total_seconds()
    assert diff_seconds == pytest.approx(300.0, abs=1.0)

    reset_settings_cache()


def test_email_sender_deliver_skips_opt_out(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession(opt_out_emails=["skip@example.com"])
    reset_settings_cache()
    monkeypatch.setenv("EMAIL_SENDING_ENABLED", "true")

    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    sender.mx_router = MagicMock()
    sender.mx_router.classify.return_value = MXResult("OTHER", [], False)
    monkeypatch.setattr(sender, "_send_via_channel", MagicMock(side_effect=AssertionError("deliver must not be called")))
    monkeypatch.setattr(
        sender,
        "_compute_scheduled_for",
        lambda session, reference=None: datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(sender, "_is_within_send_window", lambda *_: True)

    template = generator_template()
    outreach_id = sender.queue(
        company_id="c1",
        contact_id="contact1",
        to_email="skip@example.com",
        template=template,
        request_payload={"messages": []},
        session=session,
    )

    result = sender.deliver(
        outreach_id=outreach_id,
        company_id="c1",
        contact_id="contact1",
        to_email="skip@example.com",
        subject=template.subject,
        body=template.body,
        session=session,
    )

    assert result == "skipped"
    sql, params = session.calls[-1]
    assert "UPDATE outreach_messages" in sql
    assert params["status"] == "skipped"
    assert params["last_error"] == "opt_out"

    monkeypatch.delenv("EMAIL_SENDING_ENABLED", raising=False)
    reset_settings_cache()


def test_email_sender_deliver_skips_invalid_email(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    reset_settings_cache()
    monkeypatch.setenv("EMAIL_SENDING_ENABLED", "true")

    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    sender.mx_router = MagicMock()
    sender.mx_router.classify.return_value = MXResult("OTHER", [], False)
    monkeypatch.setattr(sender, "_send_via_channel", MagicMock())
    monkeypatch.setattr(sender, "_is_within_send_window", lambda *_: True)

    template = generator_template()
    outreach_id = sender.queue(
        company_id="c1",
        contact_id="contact1",
        to_email="info@example.com",
        template=template,
        request_payload=None,
        session=session,
    )

    result = sender.deliver(
        outreach_id=outreach_id,
        company_id="c1",
        contact_id="contact1",
        to_email="not-an-email",
        subject=template.subject,
        body=template.body,
        session=session,
    )

    assert result == "skipped"
    sql, params = session.calls[-1]
    assert "UPDATE outreach_messages" in sql
    assert params["status"] == "skipped"
    assert params["last_error"] == "invalid_email"

    monkeypatch.delenv("EMAIL_SENDING_ENABLED", raising=False)
    reset_settings_cache()


def test_email_sender_deliver_success(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    reset_settings_cache()
    monkeypatch.setenv("EMAIL_SENDING_ENABLED", "true")

    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    sender.mx_router = MagicMock()
    sender.mx_router.classify.return_value = MXResult("OTHER", ["mx.test"], False)
    deliver_mock = MagicMock()
    monkeypatch.setattr(sender, "_send_via_channel", deliver_mock)
    monkeypatch.setattr(
        sender,
        "_compute_scheduled_for",
        lambda session, reference=None: datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(sender, "_is_within_send_window", lambda *_: True)

    template = generator_template()
    outreach_id = sender.queue(
        company_id="c1",
        contact_id="contact1",
        to_email="hello@example.com",
        template=template,
        request_payload={"messages": []},
        session=session,
    )

    result = sender.deliver(
        outreach_id=outreach_id,
        company_id="c1",
        contact_id="contact1",
        to_email="hello@example.com",
        subject=template.subject,
        body=template.body,
        session=session,
    )

    assert result == "sent"
    deliver_mock.assert_called_once()

    sql, params = session.calls[-1]
    assert params["status"] == "sent"
    assert isinstance(params["sent_at"], datetime)
    assert params["last_error"] is None

    monkeypatch.delenv("EMAIL_SENDING_ENABLED", raising=False)
    reset_settings_cache()


def test_email_sender_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    session = DummySession()
    reset_settings_cache()

    monkeypatch.setenv("EMAIL_SENDING_ENABLED", "false")
    sender = EmailSender(session_factory=lambda: session, use_starttls=False)  # type: ignore[arg-type]
    sender.mx_router = MagicMock()
    sender.mx_router.classify.return_value = MXResult("OTHER", ["mx.test"], False)
    deliver_mock = MagicMock()
    monkeypatch.setattr(sender, "_send_via_channel", deliver_mock)
    monkeypatch.setattr(sender, "_is_within_send_window", lambda *_: True)

    result = sender.deliver(
        outreach_id="outreach-test",
        company_id="c1",
        contact_id="contact1",
        to_email="hello@example.com",
        subject="Тема",
        body="Текст",
        session=session,
    )

    assert result == "disabled"
    deliver_mock.assert_not_called()

    monkeypatch.delenv("EMAIL_SENDING_ENABLED", raising=False)
    reset_settings_cache()


def generator_template():
    company = CompanyBrief(name="Test", domain="test.ru")
    offer = OfferBrief(value_proposition="Automation")
    generator = EmailGenerator()
    return generator._fallback_template(company, offer, None)

