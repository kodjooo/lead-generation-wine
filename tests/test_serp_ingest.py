"""Тесты парсинга и сохранения результатов SERP."""

from contextlib import contextmanager
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import json

import httpx
import pytest
import respx

from app.config import get_settings
from app.modules.serp_ingest import (
    CityDetection,
    ScreeningDecision,
    SerpIngestService,
    SerpParseError,
    SiteClassificationDecision,
    detect_actual_city,
    evaluate_homepage_content,
    evaluate_serp_document,
    parse_serp_xml,
)


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


def test_evaluate_serp_document_rejects_aggregator_url() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://shopandmall.ru/torgovye-centry/centr_goroda</url>
                <domain>shopandmall.ru</domain>
                <title>Каталог торговых центров</title>
                <passages>
                  <passage>Торговые центры Краснодара</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "mall")

    assert decision.is_relevant is False
    assert decision.reason in {"negative_marker", "aggregator_url_pattern"}


def test_serp_ingest_skips_agency_excluded_domains() -> None:
    class LocalDummyResult:
        def __init__(self, value: str) -> None:
            self._value = value

        def scalar_one(self) -> str:
            return self._value

    class LocalDummySession:
        def __init__(self) -> None:
            self.calls: List[Tuple[Any, Dict[str, Any]]] = []

        def execute(self, statement: Any, params: Dict[str, Any]) -> LocalDummyResult:
            self.calls.append((statement, params))
            return LocalDummyResult("noop")

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def close(self) -> None:
            pass

    session = LocalDummySession()

    @contextmanager
    def fake_scope(_factory):  # type: ignore[override]
        try:
            yield session
            session.commit()
        finally:
            session.close()

    service = SerpIngestService(session_factory=lambda: session)
    xml_payload = """
    <response>
      <grouping>
        <group>
          <doc>
            <url>https://gk-europeya.ru/</url>
            <domain>gk-europeya.ru</domain>
            <title>ГК Европея</title>
            <passages>
              <passage>Продажа квартир от застройщика</passage>
            </passages>
          </doc>
        </group>
      </grouping>
    </response>
    """.encode("utf-8")

    with patch(
        "app.modules.serp_ingest.session_scope",
        side_effect=lambda factory: fake_scope(factory),
    ):
        inserted = service.ingest(
            "11111111-1111-1111-1111-111111111111",
            xml_payload,
            yandex_operation_id="op-789",
            query_metadata={"entity_type": "real_estate_agency", "city": "Краснодар"},
        )

    assert inserted == []
    assert session.calls == []


def test_evaluate_serp_document_marks_brand_mall_for_homepage_verification() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://galleryk.ru/</url>
                <domain>galleryk.ru</domain>
                <title>ТРЦ «Галерея Краснодар»</title>
                <passages>
                  <passage>Главная страница торгового центра</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "mall")

    assert decision.is_relevant is True
    assert decision.reason in {None, "serp_needs_homepage_verification"}


def test_evaluate_serp_document_marks_brand_agency_for_homepage_verification() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://ayax.ru/</url>
                <domain>ayax.ru</domain>
                <title>Аякс</title>
                <passages>
                  <passage>Купить квартиру в Краснодаре</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "real_estate_agency")

    assert decision.is_relevant is True
    assert decision.requires_verification is True
    assert decision.reason == "serp_needs_homepage_verification"


def test_evaluate_serp_document_boosts_network_agency_brand() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://vladis.ru/</url>
                <domain>vladis.ru</domain>
                <title>vladis.ru</title>
                <passages>
                  <passage>Федеральное агентство недвижимости</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "real_estate_agency")

    assert decision.is_relevant is True
    assert decision.score >= 3.0


def test_evaluate_homepage_content_accepts_brand_agency_with_domain_signals() -> None:
    decision = evaluate_homepage_content(
        "Купить квартиру в Краснодаре. Подбор недвижимости. Ипотека. Контакты. Специалисты по недвижимости.",
        "real_estate_agency",
        domain="vladis.ru",
    )

    assert decision.is_relevant is True
    assert decision.reason == "homepage_brand_agency"


def test_evaluate_homepage_content_allows_agency_catalog_of_objects() -> None:
    decision = evaluate_homepage_content(
        "Каталог объектов недвижимости. Купить квартиру в Краснодаре. Ипотека. Подбор недвижимости. Контакты.",
        "real_estate_agency",
        domain="novometr23.ru",
    )

    assert decision.is_relevant is True


def test_evaluate_homepage_content_rejects_agency_directory_page() -> None:
    decision = evaluate_homepage_content(
        "Каталог агентств недвижимости. Лучшие агентства. Рейтинг агентств Краснодара.",
        "real_estate_agency",
        domain="example.com",
    )

    assert decision.is_relevant is False
    assert decision.reason == "homepage_negative_marker"


def test_evaluate_serp_document_rejects_developer_for_agency_search() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://metriks.ru/</url>
                <domain>metriks.ru</domain>
                <title>МЕТРИКС Development в Краснодаре</title>
                <passages>
                  <passage>Квартиры от застройщика в жилых комплексах</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "real_estate_agency")

    assert decision.is_relevant is True
    assert decision.requires_verification is True
    assert decision.reason == "serp_needs_homepage_verification"


def test_evaluate_serp_document_keeps_unknown_mall_brand_for_homepage_check() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://krasnodar.red-square.ru/</url>
                <domain>krasnodar.red-square.ru</domain>
                <title>«Красная Площадь» в</title>
                <passages>
                  <passage>Адрес и контакты</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "mall")

    assert decision.is_relevant is True
    assert decision.requires_verification is True
    assert decision.reason == "serp_needs_homepage_verification"


def test_evaluate_serp_document_accepts_red_square_mall_brand() -> None:
    documents = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://krasnodar.red-square.ru/</url>
                <domain>krasnodar.red-square.ru</domain>
                <title>«Красная Площадь» в Краснодаре</title>
                <passages>
                  <passage>Официальный сайт торгово-развлекательного центра</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )

    decision = evaluate_serp_document(documents[0], "mall")

    assert decision.is_relevant is True


def test_detect_actual_city_prefers_homepage() -> None:
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://example.com/contacts</url>
                <domain>example.com</domain>
                <title>Контакты</title>
                <passages>
                  <passage>Адрес торгового центра</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    detection = detect_actual_city(
        expected_city="Краснодар",
        document=document,
        homepage_content="г. Краснодар, ул. Красная, 10. Контакты торгового центра.",
    )

    assert detection.detected_city == "Краснодар"
    assert detection.source == "homepage"
    assert detection.score >= 3.0


@respx.mock
def test_site_classification_llm_marks_tenant_inside_mall(monkeypatch: pytest.MonkeyPatch) -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_PROVIDER", "openai")
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    response_json = {
        "object": "response",
        "output_text": (
            '{"site_verdict":"mall_tenant_site","detected_city":"Краснодар",'
            '"confidence":0.92,"reason":"Сайт описывает детский центр внутри ТРЦ"}'
        ),
    }
    respx.post("https://api.openai.com/v1/responses").mock(
        return_value=httpx.Response(200, json=response_json)
    )

    service = SerpIngestService(session_factory=lambda: None)  # type: ignore[arg-type]
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://krasnodar.city.kidburg.ru/</url>
                <domain>krasnodar.city.kidburg.ru</domain>
                <title>КидБург в Краснодаре</title>
                <passages>
                  <passage>Город профессий в ТРЦ</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    classification = service._maybe_classify_site_with_llm(
        expected_city="Краснодар",
        expected_entity_type="mall",
        document=document,
        homepage_content="КидБург в ТРЦ Красная площадь. Билеты, расписание, контакты.",
        detection=CityDetection(detected_city=None, score=0.0, source=None),
    )

    assert classification is not None
    assert classification.site_verdict == "mall_tenant_site"
    assert classification.detected_city == "Краснодар"
    assert classification.confidence == 0.92
    assert service._is_llm_verdict_accepted("mall", classification.site_verdict) is False
    get_settings.cache_clear()  # type: ignore[attr-defined]


@respx.mock
def test_site_classification_llm_runs_for_uncertain_agency_even_with_city(monkeypatch: pytest.MonkeyPatch) -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_PROVIDER", "openai")
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    response_json = {
        "object": "response",
        "output_text": (
            '{"site_verdict":"official_real_estate_agency_site","detected_city":"Краснодар",'
            '"confidence":0.89,"reason":"Сайт агентства недвижимости с каталогом объектов и услугами риэлторов"}'
        ),
    }
    route = respx.post("https://api.openai.com/v1/responses").mock(
        return_value=httpx.Response(200, json=response_json)
    )

    service = SerpIngestService(session_factory=lambda: None)  # type: ignore[arg-type]
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://verno.pro/</url>
                <domain>verno.pro</domain>
                <title>VERNO</title>
                <passages>
                  <passage>Недвижимость в Краснодаре</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    classification = service._maybe_classify_site_with_llm(
        expected_city="Краснодар",
        expected_entity_type="real_estate_agency",
        document=document,
        homepage_content="Каталог объектов. Купить квартиру в Краснодаре. Ипотека. Контакты.",
        detection=CityDetection(detected_city="Краснодар", score=4.0, source="homepage"),
        serp_decision=ScreeningDecision(True, 2.5, "serp_needs_homepage_verification", True),
        homepage_decision=ScreeningDecision(True, 5.0, None),
    )

    assert route.called is True
    assert classification is not None
    assert classification.site_verdict == "official_real_estate_agency_site"
    assert classification.detected_city == "Краснодар"
    assert service._is_llm_verdict_accepted("real_estate_agency", classification.site_verdict) is True
    request_payload = json.loads(route.calls[0].request.content.decode("utf-8"))
    assert request_payload["input"][0]["content"][0]["type"] == "input_text"
    user_payload = json.loads(request_payload["input"][1]["content"][0]["text"])
    assert "homepage_screening" in user_payload
    assert "serp_screening" in user_payload
    get_settings.cache_clear()  # type: ignore[attr-defined]


def test_site_classification_llm_skips_confident_agency(monkeypatch: pytest.MonkeyPatch) -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_PROVIDER", "openai")
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    service = SerpIngestService(session_factory=lambda: None)  # type: ignore[arg-type]
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://centrug.ru/</url>
                <domain>centrug.ru</domain>
                <title>Центр-Юг</title>
                <passages>
                  <passage>Агентство недвижимости в Краснодаре</passage>
                </passages>
                <properties>
                  <property name="lang">ru</property>
                </properties>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    classification = service._maybe_classify_site_with_llm(
        expected_city="Краснодар",
        expected_entity_type="real_estate_agency",
        document=document,
        homepage_content="Агентство недвижимости. Купить, продать, ипотека, специалисты по недвижимости, контакты.",
        detection=CityDetection(detected_city="Краснодар", score=4.0, source="homepage"),
        serp_decision=ScreeningDecision(True, 8.0, None, False),
        homepage_decision=ScreeningDecision(True, 12.0, None, False),
    )

    assert classification is None
    get_settings.cache_clear()  # type: ignore[attr-defined]


@respx.mock
def test_site_classification_llm_uses_gateway_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_PROVIDER", "gateway")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MODEL", "gpt-5-mini")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_GATEWAY_URL", "https://llm-gateway.example.com")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY", "gateway-secret")

    route = respx.post("https://llm-gateway.example.com/v1/openai/responses").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "resp-gateway",
                "object": "response",
                "detected_city": "Краснодар",
                "model": "gpt-5-mini",
                "output_text": (
                    '{"site_verdict":"official_real_estate_agency_site",'
                    '"detected_city":"\\u041a\\u0440\\u0430\\u0441\\u043d\\u043e\\u0434\\u0430\\u0440","confidence":0.94,'
                    '"reason":"Gateway classified the site as an agency"}'
                ),
            },
        )
    )

    service = SerpIngestService(session_factory=lambda: None)  # type: ignore[arg-type]
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://verno.pro/</url>
                <domain>verno.pro</domain>
                <title>VERNO</title>
                <passages>
                  <passage>Недвижимость в Краснодаре</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    classification = service._maybe_classify_site_with_llm(
        expected_city="Краснодар",
        expected_entity_type="real_estate_agency",
        document=document,
        homepage_content="Каталог объектов. Купить квартиру в Краснодаре. Ипотека. Контакты.",
        detection=CityDetection(detected_city="Краснодар", score=4.0, source="homepage"),
        serp_decision=ScreeningDecision(True, 2.5, "serp_needs_homepage_verification", True),
        homepage_decision=ScreeningDecision(True, 5.0, None),
    )

    assert route.called is True
    assert classification is not None
    assert classification.site_verdict == "official_real_estate_agency_site"
    assert classification.detected_city == "Краснодар"
    assert classification.confidence == 0.94
    request_payload = json.loads(route.calls[0].request.content.decode("utf-8"))
    assert request_payload["model"] == "gpt-5-mini"
    assert request_payload["text"]["format"]["name"] == "SiteClassification"
    assert request_payload["text"]["format"]["strict"] is True
    assert request_payload["text"]["format"]["schema"]["additionalProperties"] is False
    assert request_payload["text"]["format"]["schema"]["properties"]["site_verdict"]["type"] == "string"
    assert request_payload["input"][0]["content"][0]["type"] == "input_text"
    user_payload = json.loads(request_payload["input"][1]["content"][0]["text"])
    assert user_payload["domain"] == "verno.pro"
    assert route.calls[0].request.headers["Authorization"] == "Bearer gateway-secret"
    get_settings.cache_clear()  # type: ignore[attr-defined]


@respx.mock
def test_site_classification_llm_retries_after_transient_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_PROVIDER", "openai")
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    route = respx.post("https://api.openai.com/v1/responses").mock(
        side_effect=[
            httpx.Response(429, json={"error": {"message": "rate limit"}}),
            httpx.Response(
                200,
                json={
                    "object": "response",
                    "output_text": (
                        '{"site_verdict":"official_real_estate_agency_site","detected_city":"Краснодар",'
                        '"confidence":0.91,"reason":"Повторный вызов удался"}'
                    ),
                },
            ),
        ]
    )

    service = SerpIngestService(session_factory=lambda: None)  # type: ignore[arg-type]
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://ayax.ru/</url>
                <domain>ayax.ru</domain>
                <title>Аякс</title>
                <passages>
                  <passage>Недвижимость в Краснодаре</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    classification = service._request_site_classification_llm(
        expected_city="Краснодар",
        expected_entity_type="real_estate_agency",
        document=document,
        homepage_content="Агентство недвижимости. Купить квартиру. Ипотека. Контакты.",
        serp_decision=ScreeningDecision(True, 2.0, "serp_needs_homepage_verification", True),
        homepage_decision=ScreeningDecision(True, 5.0, None),
    )

    assert route.call_count == 2
    assert classification.site_verdict == "official_real_estate_agency_site"
    assert classification.detected_city == "Краснодар"
    assert classification.confidence == 0.91
    get_settings.cache_clear()  # type: ignore[attr-defined]


@respx.mock
def test_site_classification_llm_retries_invalid_payload_three_times_then_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_settings.cache_clear()  # type: ignore[attr-defined]
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_PROVIDER", "openai")
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_URL", raising=False)
    monkeypatch.delenv("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    route = respx.post("https://api.openai.com/v1/responses").mock(
        side_effect=[
            httpx.Response(200, json={"object": "response", "output_text": "not-json"}),
            httpx.Response(200, json={"object": "response", "output_text": "{bad json"}),
            httpx.Response(200, json={"object": "response", "output_text": "```json\nstill-bad\n```"}),
        ]
    )

    service = SerpIngestService(session_factory=lambda: None)  # type: ignore[arg-type]
    document = parse_serp_xml(
        """
        <response>
          <grouping>
            <group>
              <doc>
                <url>https://vladis.ru/</url>
                <domain>vladis.ru</domain>
                <title>Владис</title>
                <passages>
                  <passage>Агентство недвижимости</passage>
                </passages>
              </doc>
            </group>
          </grouping>
        </response>
        """.encode("utf-8")
    )[0]

    classification = service._request_site_classification_llm(
        expected_city="Краснодар",
        expected_entity_type="real_estate_agency",
        document=document,
        homepage_content="Агентство недвижимости. Ипотека. Контакты.",
        serp_decision=ScreeningDecision(True, 2.0, "serp_needs_homepage_verification", True),
        homepage_decision=ScreeningDecision(True, 5.0, None),
    )

    assert route.call_count == 3
    assert classification.site_verdict is None
    assert classification.detected_city is None
    assert classification.confidence == 0.0
    get_settings.cache_clear()  # type: ignore[attr-defined]


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
    ), patch.object(
        service,
        "_get_homepage_content",
        return_value="Торгово-развлекательный центр. Контакты и аренда.",
    ), patch.object(
        service,
        "_evaluate_homepage",
        return_value=ScreeningDecision(True, 7.5, None),
    ), patch.object(
        service,
        "_maybe_classify_site_with_llm",
        return_value=SiteClassificationDecision(
            site_verdict="official_mall_site",
            detected_city="Москва",
            confidence=0.93,
            reason="Главная страница похожа на официальный сайт ТЦ.",
        ),
    ):
        inserted = service.ingest(
            "11111111-1111-1111-1111-111111111111",
            SAMPLE_XML,
            yandex_operation_id="op-123",
            query_metadata={"entity_type": "mall", "city": "Москва"},
        )

    assert len(inserted) == 2
    assert inserted[0] == "id-1"
    assert session.committed is True
    assert session.closed is True
    assert len(session.calls) == 4

    first_stmt_text = session.calls[0][0].text
    second_stmt_text = session.calls[1][0].text
    assert "INSERT INTO serp_results" in first_stmt_text
    assert "INSERT INTO companies" in second_stmt_text
    assert "ON CONFLICT (canonical_domain) WHERE canonical_domain IS NOT NULL" in second_stmt_text

    params_result = session.calls[0][1]
    assert params_result["domain"] == "example.com"
    assert params_result["operation_id"] == "11111111-1111-1111-1111-111111111111"
    assert params_result["metadata"].startswith("{")
    assert '"yandex_operation_id": "op-123"' in params_result["metadata"]
    assert '"llm_site_verdict": "official_mall_site"' in params_result["metadata"]
    assert '"llm_status": "success"' in params_result["metadata"]

    params_company = session.calls[1][1]
    assert params_company["domain"] == "example.com"
    assert params_company["website_url"].startswith("https://example.com")
    assert params_company["industry"] == "mall"
    assert params_company["actual_region"] == "Москва"
    assert '"llm_status": "success"' in params_company["attributes"]


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
    ), patch.object(
        service,
        "_verify_candidate_homepage",
        return_value=ScreeningDecision(False, 0.0, "homepage_negative_marker"),
    ):
        inserted = service.ingest(
            "11111111-1111-1111-1111-111111111111",
            EXCLUDED_XML,
            yandex_operation_id="op-456",
        )

    assert inserted == []
    assert session.calls == []


def test_build_llm_tracking_payload_marks_error_for_empty_verdict() -> None:
    service = SerpIngestService()

    payload = service._build_llm_tracking_payload(
        SiteClassificationDecision(
            site_verdict=None,
            detected_city=None,
            confidence=0.0,
            reason=None,
        )
    )

    assert payload["llm_status"] == "error"
    assert payload["llm_provider"] in {"openai", "gateway"}
    assert payload["llm_confidence"] == 0.0
    assert "llm_checked_at" in payload
    assert "llm_site_verdict" not in payload


