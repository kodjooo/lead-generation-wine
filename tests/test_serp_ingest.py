"""Тесты парсинга и сохранения результатов SERP."""

from contextlib import contextmanager
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import pytest

from app.modules.serp_ingest import SerpIngestService, SerpParseError, parse_serp_xml


SAMPLE_XML = """
<response>
  <grouping>
    <group>
      <doc>
        <url>https://example.com/products</url>
        <domain>example.com</domain>
        <title>Example Mall официальный сайт</title>
        <passages>
          <passage>Торгово-развлекательный центр в Москве</passage>
        </passages>
        <properties>
          <property name="lang">ru</property>
        </properties>
      </doc>
    </group>
    <group>
      <doc>
        <url>beta.ru</url>
        <title>Beta недвижимость официальный сайт</title>
        <passages>
          <passage>Агентство недвижимости полного цикла</passage>
        </passages>
      </doc>
    </group>
  </grouping>
</response>
""".encode("utf-8")

EXCLUDED_XML = """
<response>
  <grouping>
    <group>
      <doc>
        <url>https://support.avito.ru/help</url>
        <domain>support.avito.ru</domain>
        <title>Avito Support</title>
        <passages>
          <passage>Свяжитесь с нами</passage>
        </passages>
      </doc>
    </group>
  </grouping>
</response>
""".encode("utf-8")


def test_parse_serp_xml_extracts_documents() -> None:
    documents = parse_serp_xml(SAMPLE_XML)
    assert len(documents) == 2
    assert documents[0].domain == "example.com"
    assert documents[0].language == "ru"
    assert documents[1].url == "https://beta.ru/"
    assert documents[1].snippet.startswith("Агентство")


def test_parse_serp_xml_invalid_payload() -> None:
    with pytest.raises(SerpParseError):
        parse_serp_xml(b"<broken>")


class DummyResult:
    def __init__(self, value: str) -> None:
        self._value = value

    def scalar_one(self) -> str:
        return self._value


class DummySession:
    def __init__(self) -> None:
        self.calls: List[Tuple[Any, Dict[str, Any]]] = []
        self.committed = False
        self.closed = False

    def execute(self, statement: Any, params: Dict[str, Any]) -> DummyResult:
        self.calls.append((statement, params))
        return DummyResult(f"id-{len(self.calls)}")

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def test_serp_ingest_persists_results_and_companies() -> None:
    session = DummySession()

    @contextmanager
    def fake_scope(_factory):  # type: ignore[override]
        try:
            yield session
            session.commit()
        finally:
            session.close()

    service = SerpIngestService(session_factory=lambda: session)

    with patch(
        "app.modules.serp_ingest.session_scope",
        side_effect=lambda factory: fake_scope(factory),
    ):
        inserted = service.ingest(
            "11111111-1111-1111-1111-111111111111",
            SAMPLE_XML,
            yandex_operation_id="op-123",
            query_metadata={"entity_type": "mall", "city": "Москва"},
        )

    assert len(inserted) == 1
    assert inserted[0] == "id-1"
    assert session.committed is True
    assert session.closed is True
    assert len(session.calls) == 2

    first_stmt_text = session.calls[0][0].text
    second_stmt_text = session.calls[1][0].text
    assert "INSERT INTO serp_results" in first_stmt_text
    assert "INSERT INTO companies" in second_stmt_text

    params_result = session.calls[0][1]
    assert params_result["domain"] == "example.com"
    assert params_result["operation_id"] == "11111111-1111-1111-1111-111111111111"
    assert params_result["metadata"].startswith("{")
    assert '"yandex_operation_id": "op-123"' in params_result["metadata"]

    params_company = session.calls[1][1]
    assert params_company["domain"] == "example.com"
    assert params_company["website_url"].startswith("https://example.com")
    assert params_company["industry"] == "mall"


def test_serp_ingest_skips_excluded_domains() -> None:
    session = DummySession()

    @contextmanager
    def fake_scope(_factory):  # type: ignore[override]
        try:
            yield session
            session.commit()
        finally:
            session.close()

    service = SerpIngestService(session_factory=lambda: session)

    with patch(
        "app.modules.serp_ingest.session_scope",
        side_effect=lambda factory: fake_scope(factory),
    ):
        inserted = service.ingest(
            "11111111-1111-1111-1111-111111111111",
            EXCLUDED_XML,
            yandex_operation_id="op-456",
        )

    assert inserted == []
    assert session.calls == []
