"""Клиент для работы с Yandex Search API в deferred-режиме."""

from __future__ import annotations

import base64
import logging
import binascii
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Deque, Dict, Iterable, Optional

import httpx
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger("app.yandex_deferred")

SEARCH_ASYNC_URL = "https://searchapi.api.cloud.yandex.net/v2/web/searchAsync"
OPERATIONS_URL = "https://operation.api.cloud.yandex.net/operations"


class YandexAPIError(RuntimeError):
    """Базовое исключение для ошибок Yandex Search API."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class NightWindowViolation(YandexAPIError):
    """Попытка создать deferred-запрос вне ночного окна."""


class OperationTimeout(YandexAPIError):
    """Превышено время ожидания завершения операции."""


class InvalidResponseError(YandexAPIError):
    """Ответ API не содержит ожидаемых данных."""


@dataclass
class RateLimitRule:
    """Правило ограничения запросов за фиксированное окно."""

    limit: int
    window: timedelta
    events: Deque[datetime]


@dataclass
class RateLimitConfig:
    """Набор ограничений для API-методов."""

    per_second: int
    per_minute: int
    per_hour: int

    def build_rules(self) -> Iterable[RateLimitRule]:
        """Создаёт правила с отдельными очередями событий."""
        return (
            RateLimitRule(self.per_second, timedelta(seconds=1), deque()),
            RateLimitRule(self.per_minute, timedelta(minutes=1), deque()),
            RateLimitRule(self.per_hour, timedelta(hours=1), deque()),
        )


@dataclass
class DeferredQueryParams:
    """Параметры запроса поиска."""

    query_text: str
    region: int = 225
    search_type: str = "SEARCH_TYPE_RU"
    localization: str = "LOCALIZATION_RU"
    page: int = 0
    fix_typo_mode: str = "FIX_TYPO_MODE_ON"
    sort_mode: str = "SORT_MODE_BY_RELEVANCE"
    sort_order: str = "SORT_ORDER_DESC"
    group_mode: str = "GROUP_MODE_DEEP"
    groups_on_page: int = 50
    docs_in_group: int = 1
    max_passages: int = 3
    response_format: str = "FORMAT_XML"
    user_agent: Optional[str] = None

    def to_payload(self, folder_id: str) -> Dict[str, Any]:
        """Преобразует параметры в тело POST запроса."""
        payload: Dict[str, Any] = {
            "query": {
                "search_type": self.search_type,
                "query_text": self.query_text,
                "family_mode": "FAMILY_MODE_MODERATE",
                "page": self.page,
                "fix_typo_mode": self.fix_typo_mode,
            },
            "sort_spec": {
                "sort_mode": self.sort_mode,
                "sort_order": self.sort_order,
            },
            "group_spec": {
                "group_mode": self.group_mode,
                "groups_on_page": self.groups_on_page,
                "docs_in_group": self.docs_in_group,
            },
            "max_passages": self.max_passages,
            "region": str(self.region),
            "l10n": self.localization,
            "folder_id": folder_id,
            "response_format": self.response_format,
        }

        if self.user_agent:
            payload["user_agent"] = self.user_agent

        return payload


@dataclass
class OperationResponse:
    """Структурированный ответ операции deferred-поиска."""

    id: str
    done: bool
    response: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OperationResponse":
        """Создаёт объект из словаря ответа API."""
        return cls(
            id=data.get("id", ""),
            done=bool(data.get("done", False)),
            response=data.get("response"),
            error=data.get("error"),
        )

    def raw_data_base64(self) -> Optional[str]:
        """Возвращает Base64 с XML/HTML результатом."""
        if not self.response:
            return None
        return self.response.get("rawData")

    def decode_raw_data(self) -> bytes:
        """Декодирует Base64 и возвращает сырые данные выдачи."""
        raw_base64 = self.raw_data_base64()
        if not raw_base64:
            raise InvalidResponseError("Поле response.rawData отсутствует в ответе.")
        try:
            return base64.b64decode(raw_base64)
        except (ValueError, binascii.Error) as exc:  # type: ignore[name-defined]
            raise InvalidResponseError("Не удалось декодировать rawData.") from exc


class YandexDeferredClient:
    """Высокоуровневый клиент для создания и отслеживания deferred-запросов."""

    def __init__(
        self,
        *,
        iam_token: str | None = None,
        token_provider: Optional[Callable[[], str]] = None,
        folder_id: str,
        timezone: str = "Europe/Moscow",
        enforce_night_window: bool = True,
        poll_interval_seconds: int = 60,
        max_wait_minutes: int = 180,
        create_limits: RateLimitConfig | None = None,
        status_limits: RateLimitConfig | None = None,
        timeout: float = 10.0,
        sleep_func: Callable[[float], None] | None = None,
        now_func: Callable[[], datetime] | None = None,
    ) -> None:
        self._iam_token = iam_token
        self._token_resolver = token_provider
        self.folder_id = folder_id
        self.timezone = ZoneInfo(timezone)
        self.enforce_night_window = enforce_night_window
        self.poll_interval = max(1, poll_interval_seconds)
        self.max_wait = timedelta(minutes=max_wait_minutes)
        self.timeout = timeout
        self._sleep = sleep_func or time.sleep
        self._now_func = now_func

        self._create_limits = tuple(
            (create_limits or RateLimitConfig(10, 600, 35000)).build_rules()
        )
        self._status_limits = tuple(
            (status_limits or RateLimitConfig(10, 600, 35000)).build_rules()
        )

    def _now(self) -> datetime:
        return self._now_func() if self._now_func else datetime.now(self.timezone)

    def _headers(self) -> Dict[str, str]:
        token = self._resolve_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _resolve_token(self) -> str:
        if self._token_resolver:
            token = self._token_resolver()
            if token:
                return token
        if self._iam_token:
            return self._iam_token
        raise YandexAPIError("IAM токен не задан.")

    def _respect_limits(self, rules: Iterable[RateLimitRule]) -> None:
        current_time = self._now()
        for rule in rules:
            while rule.events and (current_time - rule.events[0]) > rule.window:
                rule.events.popleft()

            if len(rule.events) >= rule.limit:
                wait_for = (rule.events[0] + rule.window) - current_time
                seconds = max(wait_for.total_seconds(), 0)
                if seconds > 0:
                    LOGGER.debug(
                        "Превышен лимит %s запросов за %s. Ждём %.2f c.",
                        rule.limit,
                        rule.window,
                        seconds,
                    )
                    self._sleep(seconds)
                current_time = self._now()

            rule.events.append(current_time)

    def _ensure_night_window(self) -> None:
        if not self.enforce_night_window:
            return
        now_local = self._now()
        if not (0 <= now_local.hour < 8):
            raise NightWindowViolation(
                "Создание deferred-запросов разрешено только в ночное окно (00:00-07:59)."
            )

    def create_deferred_search(
        self,
        params: DeferredQueryParams,
        extra: Optional[Dict[str, Any]] = None,
    ) -> OperationResponse:
        """Создаёт deferred-запрос и возвращает ответ с operation_id."""
        self._ensure_night_window()
        self._respect_limits(self._create_limits)

        payload = params.to_payload(self.folder_id)
        if extra:
            payload.update(extra)

        LOGGER.debug("Создание deferred-запроса: %s", payload)

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                SEARCH_ASYNC_URL,
                json=payload,
                headers=self._headers(),
            )

        if response.status_code >= 400:
            LOGGER.error(
                "Ошибка создания deferred-запроса: %s %s",
                response.status_code,
                response.text,
            )
            raise YandexAPIError(
                f"Ошибка создания deferred-запроса: {response.status_code}",
                status_code=response.status_code,
            )

        return OperationResponse.from_dict(response.json())

    def get_operation(self, operation_id: str) -> OperationResponse:
        """Возвращает текущее состояние операции."""
        self._respect_limits(self._status_limits)
        url = f"{OPERATIONS_URL}/{operation_id}"
        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(url, headers=self._headers())

        if response.status_code >= 400:
            LOGGER.error(
                "Ошибка получения операции %s: %s %s",
                operation_id,
                response.status_code,
                response.text,
            )
            raise YandexAPIError(
                f"Ошибка получения операции: {response.status_code}",
                status_code=response.status_code,
            )

        return OperationResponse.from_dict(response.json())

    def wait_until_ready(
        self,
        operation_id: str,
        *,
        poll_interval_seconds: Optional[int] = None,
        timeout_minutes: Optional[int] = None,
    ) -> OperationResponse:
        """Ожидает завершения операции, периодически опрашивая её статус."""
        interval = poll_interval_seconds or self.poll_interval
        deadline = self._now() + (timedelta(minutes=timeout_minutes) if timeout_minutes else self.max_wait)

        while True:
            operation = self.get_operation(operation_id)
            if operation.done:
                return operation

            if self._now() >= deadline:
                raise OperationTimeout(
                    f"Операция {operation_id} не завершилась за отведённое время."
                )

            self._sleep(interval)
