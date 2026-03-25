"""Тесты конфигурации приложения."""

from app.config import get_settings


def test_settings_loaded_from_env(monkeypatch) -> None:
    """Проверяет, что настройки корректно читаются из окружения."""
    get_settings.cache_clear()  # type: ignore[attr-defined]

    monkeypatch.setenv("POSTGRES_HOST", "db-test")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "tester")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_DB", "leadgen_test")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/1")
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_PORT", "2525")
    monkeypatch.setenv("YANDEX_CLOUD_IAM_TOKEN", "test-token")
    monkeypatch.setenv("YANDEX_CLOUD_FOLDER_ID", "folder-test")
    monkeypatch.setenv("YANDEX_RESULTS_PROCESSING_MODE", "night_only")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_ENABLED", "true")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MODEL", "gpt-4.1-mini")
    monkeypatch.setenv("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.75")
    monkeypatch.setenv("CONTACT_ENRICH_PLAYWRIGHT_ENABLED", "true")
    monkeypatch.setenv("CONTACT_ENRICH_PLAYWRIGHT_TIMEOUT_SECONDS", "22")
    monkeypatch.setenv("EMAIL_GENERATION_ENABLED", "false")
    monkeypatch.setenv("GMAIL_SMTP_HOST", "smtp.test")
    monkeypatch.setenv("GMAIL_SMTP_PORT", "2525")
    monkeypatch.setenv("GMAIL_USER", "mailer@test")
    monkeypatch.setenv("GMAIL_PASS", "gmail-pass")
    monkeypatch.setenv("GMAIL_FROM", "Test Sender <leadgen@example.com>")
    monkeypatch.setenv("GMAIL_SMTP_TLS", "true")
    monkeypatch.setenv("YANDEX_SMTP_HOST", "smtp.yandex.test")
    monkeypatch.setenv("YANDEX_SMTP_PORT", "465")
    monkeypatch.setenv("YANDEX_USER", "yandex-user")
    monkeypatch.setenv("YANDEX_PASS", "yandex-pass")
    monkeypatch.setenv("YANDEX_FROM", "Yandex Sender <sender@yandex.ru>")
    monkeypatch.setenv("ROUTING_ENABLED", "true")
    monkeypatch.setenv("ROUTING_MX_CACHE_TTL_HOURS", "24")
    monkeypatch.setenv("ROUTING_DNS_TIMEOUT_MS", "2200")
    monkeypatch.setenv("ROUTING_DNS_RESOLVERS", "1.1.1.1,9.9.9.9")
    monkeypatch.setenv("ROUTING_RU_MX_PATTERNS", "mx.yandex.net,mx.mail.ru")
    monkeypatch.setenv("ROUTING_RU_MX_TLDS", ".ru,.su")
    monkeypatch.setenv("ROUTING_FORCE_RU_DOMAINS", "mail.ru,rambler.ru")

    settings = get_settings()

    assert settings.database.host == "db-test"
    assert settings.database.port == 5433
    assert settings.database.user == "tester"
    assert settings.database.password == "secret"
    assert settings.database.name == "leadgen_test"
    assert settings.redis_url == "redis://localhost:6379/1"
    assert settings.smtp.host == "smtp.test"
    assert settings.smtp.port == 2525
    assert settings.smtp.username == "mailer@test"
    assert settings.smtp.password == "gmail-pass"
    assert settings.smtp.sender == "leadgen@example.com"
    assert settings.smtp.sender_name == "Test Sender"
    assert settings.smtp.use_tls is True
    assert settings.smtp.use_ssl is False
    assert settings.smtp_yandex.host == "smtp.yandex.test"
    assert settings.smtp_yandex.username == "yandex-user"
    assert settings.smtp_yandex.password == "yandex-pass"
    assert settings.smtp_yandex.sender == "sender@yandex.ru"
    assert settings.routing.enabled is True
    assert settings.routing.mx_cache_ttl_hours == 24
    assert settings.routing.dns_timeout_seconds == 2.2
    assert settings.routing.dns_resolvers == ("1.1.1.1", "9.9.9.9")
    assert settings.routing.ru_mx_patterns == ("mx.yandex.net", "mx.mail.ru")
    assert settings.routing.ru_mx_tlds == (".ru", ".su")
    assert settings.routing.force_ru_domains == ("mail.ru", "rambler.ru")
    assert settings.yandex_folder_id == "folder-test"
    assert settings.yandex_iam_token == "test-token"
    assert settings.yandex_results_processing_mode == "night_only"
    assert settings.site_classification_llm_enabled is True
    assert settings.site_classification_llm_model == "gpt-4.1-mini"
    assert settings.site_classification_llm_min_confidence == 0.75
    assert settings.contact_enrich_playwright_enabled is True
    assert settings.contact_enrich_playwright_timeout_seconds == 22.0
    assert settings.email_generation_enabled is False

    get_settings.cache_clear()  # type: ignore[attr-defined]
