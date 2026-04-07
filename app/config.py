"""Загрузка конфигурации приложения из переменных окружения."""

from __future__ import annotations

import os
from dataclasses import dataclass
from email.utils import parseaddr
from functools import lru_cache
from typing import List, Sequence, Tuple

DEFAULT_RU_MX_PATTERNS: Tuple[str, ...] = (
    "1c.ru",
    "aeroflot.ru",
    "alfabank.ru",
    "beeline.ru",
    "beget.com",
    "facct.email",
    "facct.ru",
    "gazprom.ru",
    "gosuslugi.ru",
    "hh.ru",
    "kommersant.ru",
    "lancloud.ru",
    "lukoil.com",
    "magnit.ru",
    "mail.ru",
    "masterhost.ru",
    "mchost.ru",
    "megafon.ru",
    "mos.ru",
    "mts.ru",
    "netangels.ru",
    "nornik.ru",
    "novatek.ru",
    "pochta.ru",
    "proactivity.ru",
    "rambler-co.ru",
    "rambler.ru",
    "rbc.ru",
    "rosatom.ru",
    "roscosmos.ru",
    "rt.ru",
    "runity.ru",
    "russianpost.ru",
    "sber.ru",
    "sberbank.ru",
    "selectel.org",
    "sevstar.net",
    "sovcombank.ru",
    "sprinthost.ru",
    "tatneft.ru",
    "tbank.ru",
    "timeweb.ru",
    "vtb.ru",
    "vtbcapital.ru",
    "wildberries.ru",
    "x5.ru",
    "yandex.net",
    "yandex.ru",
)

DEFAULT_RU_MX_TLDS: Tuple[str, ...] = (
    ".ru",
    ".su",
    ".xn--p1ai",  # .рф
    ".xn--p1acf",  # .рус
    ".moscow",
    ".moskva",
    ".xn--80adxhks",  # .москва
)

@dataclass(frozen=True)
class DatabaseSettings:
    """Параметры подключения к базе данных."""

    host: str
    port: int
    user: str
    password: str
    name: str

    def sync_dsn(self) -> str:
        """Формирует DSN для синхронного движка SQLAlchemy."""
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass(frozen=True)
class SMTPChannelSettings:
    """Параметры SMTP-канала."""

    host: str
    port: int
    username: str
    password: str
    sender: str
    sender_name: str | None
    use_tls: bool
    use_ssl: bool

    def from_header(self) -> str:
        """Готовый заголовок From для канала."""
        if self.sender_name:
            return f"{self.sender_name} <{self.sender}>"
        return self.sender


@dataclass(frozen=True)
class RoutingSettings:
    """Настройки MX-маршрутизации."""

    enabled: bool
    mx_cache_ttl_hours: int
    dns_timeout_seconds: float
    dns_resolvers: Tuple[str, ...]
    ru_mx_patterns: Tuple[str, ...]
    ru_mx_tlds: Tuple[str, ...]
    force_ru_domains: Tuple[str, ...]


@dataclass(frozen=True)
class GoogleSheetsSettings:
    """Настройки доступа к Google Sheets."""

    sheet_id: str
    tab_name: str
    service_account_key_path: str | None
    service_account_key_json: str | None


@dataclass(frozen=True)
class SheetSyncSettings:
    """Параметры автоматической синхронизации Google Sheets."""

    enabled: bool
    interval_minutes: int
    batch_tag: str | None


@dataclass(frozen=True)
class Settings:
    """Глобальные настройки приложения."""

    timezone: str
    yandex_folder_id: str
    yandex_iam_token: str | None
    yandex_sa_key_path: str | None
    yandex_sa_key_json: str | None
    yandex_enforce_night_window: bool
    yandex_results_processing_mode: str
    openai_api_key: str
    email_generation_llm_provider: str
    email_generation_llm_model: str
    email_generation_llm_reasoning_effort: str
    email_generation_llm_timeout_seconds: float
    email_generation_llm_gateway_url: str | None
    email_generation_llm_gateway_api_key: str | None
    site_classification_llm_enabled: bool
    site_classification_llm_provider: str
    site_classification_llm_model: str
    site_classification_llm_min_confidence: float
    site_classification_llm_gateway_url: str | None
    site_classification_llm_gateway_api_key: str | None
    contact_enrich_playwright_enabled: bool
    contact_enrich_playwright_timeout_seconds: float
    email_generation_enabled: bool
    email_sending_enabled: bool
    redis_url: str
    database: DatabaseSettings
    smtp: SMTPChannelSettings
    smtp_gmail: SMTPChannelSettings
    smtp_yandex: SMTPChannelSettings
    routing: RoutingSettings
    google_sheets: GoogleSheetsSettings
    sheet_sync: SheetSyncSettings


def _env(key: str, default: str = "") -> str:
    """Возвращает значение переменной окружения или значение по умолчанию."""
    return os.getenv(key, default).strip()


def _env_bool(key: str, default: bool = False) -> bool:
    value = os.getenv(key)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(key: str, default: Sequence[str] | None = None) -> List[str]:
    value = os.getenv(key)
    if value is None:
        return list(default or [])
    separators = {",", "\n", ";"}
    buffer = []
    current = []
    for char in value:
        if char in separators:
            chunk = "".join(current).strip()
            if chunk:
                buffer.append(chunk)
            current = []
        else:
            current.append(char)
    chunk = "".join(current).strip()
    if chunk:
        buffer.append(chunk)
    return buffer


def _sender_from_combined(combined: str | None, fallback_email: str, fallback_name: str | None) -> Tuple[str, str | None]:
    if combined:
        name, email = parseaddr(combined)
        if email:
            return email, name or fallback_name
    return fallback_email, fallback_name


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Загружает настройки один раз и кэширует их для повторного использования."""
    db = DatabaseSettings(
        host=_env("POSTGRES_HOST", "db"),
        port=int(_env("POSTGRES_PORT", "5432")),
        user=_env("POSTGRES_USER", "leadgen"),
        password=_env("POSTGRES_PASSWORD", "leadgen_password"),
        name=_env("POSTGRES_DB", "leadgen"),
    )

    gmail_sender_email = _env("GMAIL_FROM_EMAIL") or _env("SMTP_FROM_EMAIL", "")
    gmail_sender_name = _env("GMAIL_FROM_NAME") or _env("SMTP_FROM_NAME") or None
    gmail_sender_email, gmail_sender_name = _sender_from_combined(
        _env("GMAIL_FROM") or None,
        gmail_sender_email,
        gmail_sender_name,
    )
    gmail = SMTPChannelSettings(
        host=_env("GMAIL_SMTP_HOST") or _env("SMTP_HOST", "smtp.gmail.com"),
        port=int(_env("GMAIL_SMTP_PORT") or _env("SMTP_PORT", "587")),
        username=_env("GMAIL_USER") or _env("SMTP_USERNAME", ""),
        password=_env("GMAIL_PASS") or _env("SMTP_PASSWORD", ""),
        sender=gmail_sender_email,
        sender_name=gmail_sender_name,
        use_tls=_env_bool("GMAIL_SMTP_TLS", True),
        use_ssl=_env_bool("GMAIL_SMTP_SSL", False),
    )

    yandex_sender_email = _env("YANDEX_FROM_EMAIL") or _env("YANDEX_USER", "")
    yandex_sender_name = _env("YANDEX_FROM_NAME") or None
    yandex_sender_email, yandex_sender_name = _sender_from_combined(
        _env("YANDEX_FROM") or None,
        yandex_sender_email,
        yandex_sender_name,
    )
    yandex = SMTPChannelSettings(
        host=_env("YANDEX_SMTP_HOST", ""),
        port=int(_env("YANDEX_SMTP_PORT", "465")),
        username=_env("YANDEX_USER"),
        password=_env("YANDEX_PASS"),
        sender=yandex_sender_email,
        sender_name=yandex_sender_name,
        use_tls=_env_bool("YANDEX_SMTP_TLS", False),
        use_ssl=_env_bool("YANDEX_SMTP_SSL", True),
    )

    routing = RoutingSettings(
        enabled=_env_bool("ROUTING_ENABLED", True),
        mx_cache_ttl_hours=int(_env("ROUTING_MX_CACHE_TTL_HOURS", "168")),
        dns_timeout_seconds=max(int(_env("ROUTING_DNS_TIMEOUT_MS", "1500")) / 1000.0, 0.1),
        dns_resolvers=tuple(_env_list("ROUTING_DNS_RESOLVERS", ["1.1.1.1", "8.8.8.8"])),
        ru_mx_patterns=tuple(_env_list("ROUTING_RU_MX_PATTERNS", list(DEFAULT_RU_MX_PATTERNS))),
        ru_mx_tlds=tuple(_env_list("ROUTING_RU_MX_TLDS", list(DEFAULT_RU_MX_TLDS))),
        force_ru_domains=tuple(_env_list("ROUTING_FORCE_RU_DOMAINS", [
            "yandex.ru",
            "yandex.com",
            "mail.ru",
            "bk.ru",
            "inbox.ru",
            "list.ru",
            "rambler.ru",
        ])),
    )

    google_sheets = GoogleSheetsSettings(
        sheet_id=_env("GOOGLE_SHEET_ID"),
        tab_name=_env("GOOGLE_SHEET_TAB", "CITIES_INPUT"),
        service_account_key_path=_env("GOOGLE_SA_KEY_FILE") or None,
        service_account_key_json=_env("GOOGLE_SA_KEY_JSON") or None,
    )

    sheet_sync = SheetSyncSettings(
        enabled=_env("SHEET_SYNC_ENABLED", "false").lower() in {"1", "true", "yes"},
        interval_minutes=int(_env("SHEET_SYNC_INTERVAL_MINUTES", "60")),
        batch_tag=_env("SHEET_SYNC_BATCH_TAG") or None,
    )

    timezone_name = _env("APP_TIMEZONE", "Europe/Moscow")
    results_processing_mode = (_env("YANDEX_RESULTS_PROCESSING_MODE", "anytime") or "anytime").lower()
    if results_processing_mode not in {"anytime", "night_only"}:
        results_processing_mode = "anytime"
    site_classification_llm_provider = (_env("SITE_CLASSIFICATION_LLM_PROVIDER", "openai") or "openai").lower()
    if site_classification_llm_provider not in {"openai", "gateway"}:
        site_classification_llm_provider = "openai"
    email_generation_llm_provider = (_env("EMAIL_GENERATION_LLM_PROVIDER", "openai") or "openai").lower()
    if email_generation_llm_provider not in {"openai", "gateway"}:
        email_generation_llm_provider = "openai"
    email_generation_llm_reasoning_effort = (_env("EMAIL_GENERATION_LLM_REASONING_EFFORT", "low") or "low").lower()
    if email_generation_llm_reasoning_effort not in {"low", "medium", "high"}:
        email_generation_llm_reasoning_effort = "low"

    return Settings(
        timezone=timezone_name,
        yandex_folder_id=_env("YANDEX_CLOUD_FOLDER_ID"),
        yandex_iam_token=_env("YANDEX_CLOUD_IAM_TOKEN") or None,
        yandex_sa_key_path=_env("YANDEX_CLOUD_SA_KEY_FILE") or None,
        yandex_sa_key_json=_env("YANDEX_CLOUD_SA_KEY_JSON") or None,
        yandex_enforce_night_window=_env_bool("YANDEX_ENFORCE_NIGHT_WINDOW", True),
        yandex_results_processing_mode=results_processing_mode,
        openai_api_key=_env("OPENAI_API_KEY"),
        email_generation_llm_provider=email_generation_llm_provider,
        email_generation_llm_model=_env("EMAIL_GENERATION_LLM_MODEL", "gpt-5"),
        email_generation_llm_reasoning_effort=email_generation_llm_reasoning_effort,
        email_generation_llm_timeout_seconds=float(
            _env("EMAIL_GENERATION_LLM_TIMEOUT_SECONDS", "60")
        ),
        email_generation_llm_gateway_url=_env("EMAIL_GENERATION_LLM_GATEWAY_URL") or None,
        email_generation_llm_gateway_api_key=_env("EMAIL_GENERATION_LLM_GATEWAY_API_KEY") or None,
        site_classification_llm_enabled=_env_bool("SITE_CLASSIFICATION_LLM_ENABLED", False),
        site_classification_llm_provider=site_classification_llm_provider,
        site_classification_llm_model=_env("SITE_CLASSIFICATION_LLM_MODEL", "gpt-4.1-mini"),
        site_classification_llm_min_confidence=float(_env("SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE", "0.6")),
        site_classification_llm_gateway_url=_env("SITE_CLASSIFICATION_LLM_GATEWAY_URL") or None,
        site_classification_llm_gateway_api_key=_env("SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY") or None,
        contact_enrich_playwright_enabled=_env_bool("CONTACT_ENRICH_PLAYWRIGHT_ENABLED", False),
        contact_enrich_playwright_timeout_seconds=float(
            _env("CONTACT_ENRICH_PLAYWRIGHT_TIMEOUT_SECONDS", "20")
        ),
        email_generation_enabled=_env_bool("EMAIL_GENERATION_ENABLED", True),
        email_sending_enabled=_env_bool("EMAIL_SENDING_ENABLED", True),
        redis_url=_env("REDIS_URL", "redis://redis:6379/0"),
        database=db,
        smtp=gmail,
        smtp_gmail=gmail,
        smtp_yandex=yandex,
        routing=routing,
        google_sheets=google_sheets,
        sheet_sync=sheet_sync,
    )
