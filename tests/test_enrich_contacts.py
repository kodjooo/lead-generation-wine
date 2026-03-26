"""Тесты обогащения контактами."""

import json

import httpx
import respx

from app.modules.enrich_contacts import ContactEnricher


class DummyResult:
    def __init__(self, value: str) -> None:
        self._value = value

    def scalar_one(self) -> str:
        return self._value


class DummySession:
    def __init__(self) -> None:
        self.calls = []
        self.counter = 0

    def execute(self, statement, params):  # noqa: ANN001
        sql = statement.text if hasattr(statement, "text") else str(statement)
        self.calls.append((sql, params))
        if "INSERT INTO contacts" in sql:
            self.counter += 1
            return DummyResult(f"contact-{self.counter}")
        return DummyResult("noop")

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def test_extract_contacts_from_html() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]
    html = """
    <html>
      <body>
        <a href="mailto:sales@example.com">Sales</a>
        <a href="tel:+7 (495) 123-45-67">Позвонить</a>
        <p>Общий e-mail: info@example.com</p>
        <p>Телефон офиса: +7 812 000-11-22</p>
      </body>
    </html>
    """

    contacts = list(enricher._extract_contacts_from_html(html, "https://example.com"))

    emails = [c for c in contacts if c.contact_type == "email"]
    assert len(emails) == 2
    assert emails[0].value.lower() == "sales@example.com"
    assert emails[1].value.lower() == "info@example.com"


def test_extract_contacts_skips_invalid_mailto() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]
    html = """
    <html>
      <body>
        <a href="mailto:+74951234567">Позвонить</a>
      </body>
    </html>
    """

    contacts = list(enricher._extract_contacts_from_html(html, "https://example.com"))

    assert contacts == []


def test_extract_contacts_finds_text_email() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]
    html = """
    <html>
      <body>
        <p>Для связи пишите на mall@example.com</p>
      </body>
    </html>
    """

    contacts = list(enricher._extract_contacts_from_html(html, "https://example.com"))

    assert len(contacts) == 1
    assert contacts[0].value == "mall@example.com"
    assert contacts[0].quality_score == 0.8


def test_extract_contacts_finds_obfuscated_email() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]
    html = """
    <html>
      <body>
        <p>Для связи: rent [at] mall-example [dot] ru</p>
      </body>
    </html>
    """

    contacts = list(enricher._extract_contacts_from_html(html, "https://example.com"))

    assert len(contacts) == 1
    assert contacts[0].value == "rent@mall-example.ru"
    assert contacts[0].quality_score == 0.7


@respx.mock
def test_enrich_company_persists_contacts() -> None:
    session = DummySession()
    enricher = ContactEnricher(session_factory=lambda: session, sleep_func=lambda _: None)  # type: ignore[arg-type]

    respx.get("https://site.com/").mock(
        return_value=httpx.Response(
            200,
            text="""
            <html>
              <body>
                <h1>Digital агентство</h1>
                <a href=\"mailto:HELLO@site.com\">Напишите нам</a>
                <a href=\"tel:+7 (900) 123-45-67\">Позвонить</a>
                <p>Резервный e-mail: Sales@site.com</p>
                <p>Телефон офиса: 8 800 555 35 35</p>
                <p>Иностранный номер: +1 202 555 0199</p>
              </body>
            </html>
            """,
        )
    )
    for suffix in [
        "contact",
        "contacts",
        "contact-us",
        "about",
        "about-us",
        "kontakty",
        "contacts/",
        "kontakty/",
        "arenda",
        "leasing",
        "rent",
        "team",
        "offices",
        "services",
    ]:
        respx.get(f"https://site.com/{suffix}").mock(return_value=httpx.Response(404, text="not found"))

    inserted = enricher.enrich_company("company-1", "site.com", session=session)

    assert inserted == ["contact-1", "contact-2"]
    # первый вызов — обновление companies с homepage_excerpt
    assert "UPDATE companies" in session.calls[0][0]
    insert_calls = [call for call in session.calls if "INSERT INTO contacts" in call[0]]
    assert len(insert_calls) == 2
    first_insert = insert_calls[0][1]
    second_insert = insert_calls[1][1]
    assert first_insert["value"] == "sales@site.com"
    assert first_insert["is_primary"] is True
    assert second_insert["value"] == "hello@site.com"
    assert second_insert["is_primary"] is False
    company_email_calls = [call for call in session.calls if "primary_email_status" in call[0]]
    assert company_email_calls
    assert company_email_calls[-1][1]["primary_email"] == "sales@site.com"
    assert company_email_calls[-1][1]["primary_email_status"] == "identified"
    status_calls = [call for call in session.calls if "SET status" in call[0]]
    assert status_calls
    assert status_calls[-1][1]["status"] == "contacts_ready"


@respx.mock
def test_enrich_company_marks_not_found() -> None:
    session = DummySession()
    enricher = ContactEnricher(session_factory=lambda: session, sleep_func=lambda _: None)  # type: ignore[arg-type]

    respx.get("https://empty.com/").mock(
        return_value=httpx.Response(
            200,
            text="""
            <html>
              <body>
                <h1>О компании</h1>
                <p>Без явных контактных email.</p>
              </body>
            </html>
            """,
            )
        )

    for suffix in ["contact", "contacts", "contact-us", "about", "about-us", "kontakty", "contacts/", "kontakty/", "arenda", "leasing", "rent"]:
        respx.get(f"https://empty.com/{suffix}").mock(return_value=httpx.Response(404, text="not found"))

    inserted = enricher.enrich_company("company-2", "empty.com", session=session)

    assert inserted == []
    company_email_calls = [call for call in session.calls if "primary_email_status" in call[0]]
    assert company_email_calls
    assert company_email_calls[-1][1]["primary_email"] is None
    assert company_email_calls[-1][1]["primary_email_status"] == "not_found"
    status_calls = [call for call in session.calls if "SET status" in call[0]]
    assert status_calls
    assert status_calls[-1][1]["status"] == "contacts_not_found"


def test_sanitize_excerpt_removes_control_chars() -> None:
    session = DummySession()
    enricher = ContactEnricher(session_factory=lambda: session, sleep_func=lambda _: None)  # type: ignore[arg-type]

    dirty_html = "<html><body>Привет\u0000 мир\u0008!</body></html>"
    enricher._save_homepage_excerpt(session, "company-3", dirty_html)

    update_call = next(call for call in session.calls if "UPDATE companies" in call[0])
    payload = update_call[1]["patch"]
    data = json.loads(payload)
    assert data["homepage_excerpt"] == "Привет мир!"
    assert "\u0000" not in data["homepage_excerpt"]


@respx.mock
def test_fetch_html_handles_bot_challenge() -> None:
    session = DummySession()
    enricher = ContactEnricher(
        session_factory=lambda: session,  # type: ignore[arg-type]
        sleep_func=lambda _: None,
        max_retries=1,
    )

    respx.get("https://challenge.com/").mock(
        return_value=httpx.Response(403, text="<html>Verify you are human</html>")
    )

    html = enricher._fetch_html("https://challenge.com/")

    assert html == ""


def test_build_candidate_urls_is_industry_aware() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]

    mall_urls = enricher._build_candidate_urls("https://mall.example/", "mall")
    agency_urls = enricher._build_candidate_urls("https://agency.example/", "real_estate_agency")

    assert "https://mall.example/arendatoram" in mall_urls
    assert "https://mall.example/partners" in mall_urls
    assert "https://agency.example/team" in agency_urls
    assert "https://agency.example/offices" in agency_urls


def test_discover_priority_links_finds_internal_contact_pages() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]
    html = """
    <html>
      <body>
        <a href="/team">Команда</a>
        <a href="/contacts">Контакты</a>
        <a href="https://external.example/contact">External</a>
      </body>
    </html>
    """

    links = enricher._discover_priority_links(
        html,
        current_url="https://agency.example/",
        base_url="https://agency.example/",
        industry="real_estate_agency",
    )

    assert "https://agency.example/team" in links
    assert "https://agency.example/contacts" in links
    assert all("external.example" not in link for link in links)


def test_rank_contacts_prefers_leasing_email_for_mall() -> None:
    enricher = ContactEnricher(session_factory=lambda: None, sleep_func=lambda _: None)  # type: ignore[arg-type]
    contacts = [
        type("Record", (), {"value": "info@example.com", "quality_score": 1.0, "source_url": "https://mall.example/contact", "origin": "mailto"})(),
        type("Record", (), {"value": "leasing@example.com", "quality_score": 0.8, "source_url": "https://mall.example/leasing", "origin": "text"})(),
    ]

    ranked = enricher._rank_contacts(contacts, industry="mall")

    assert ranked[0].value == "leasing@example.com"


def test_enrich_company_uses_rendered_html_fallback() -> None:
    session = DummySession()
    enricher = ContactEnricher(
        session_factory=lambda: session,  # type: ignore[arg-type]
        sleep_func=lambda _: None,
        playwright_enabled=True,
    )

    enricher._fetch_html = lambda url: "<html><body><p>No contacts</p></body></html>"  # type: ignore[method-assign]
    enricher._fetch_rendered_html = lambda url: (  # type: ignore[method-assign]
        '<html><body><a href="mailto:leasing@example.com">Leasing</a></body></html>'
    )

    inserted = enricher.enrich_company("company-rendered", "rendered.example", industry="mall", session=session)

    assert inserted == ["contact-1"]
    insert_calls = [call for call in session.calls if "INSERT INTO contacts" in call[0]]
    assert len(insert_calls) == 1
    assert insert_calls[0][1]["value"] == "leasing@example.com"
