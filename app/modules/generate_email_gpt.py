"""Генерация персонализированных писем с помощью LLM."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from string import Template
from typing import Dict, List, Optional

import httpx

from app.config import get_settings

LOGGER = logging.getLogger("app.generate_email")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
EMAIL_GENERATION_GATEWAY_PATH = "/v1/openai/responses"
PROMPTS_DIR = Path(__file__).resolve().parents[1] / "prompts"
PROMPT_PATHS = {
    "mall": {
        "system": PROMPTS_DIR / "email_generation_mall_system_prompt.txt",
        "user": PROMPTS_DIR / "email_generation_mall_user_prompt.txt",
    },
    "real_estate_agency": {
        "system": PROMPTS_DIR / "email_generation_agency_system_prompt.txt",
        "user": PROMPTS_DIR / "email_generation_agency_user_prompt.txt",
    },
}


def _extract_responses_output_text(payload: Dict[str, object]) -> Optional[str]:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = payload.get("output")
    if not isinstance(output, list):
        return None

    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            text_value = part.get("text")
            if isinstance(text_value, str) and text_value.strip():
                chunks.append(text_value)

    if not chunks:
        return None
    return "\n".join(chunks)


@dataclass
class CompanyBrief:
    """Минимальное описание компании для письма."""

    domain: str
    name: Optional[str] = None
    entity_type: Optional[str] = None
    industry: Optional[str] = None
    highlights: List[str] = field(default_factory=list)


@dataclass
class ContactBrief:
    """Информация о контактном лице."""

    name: Optional[str] = None
    role: Optional[str] = None
    emails: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)


@dataclass
class OfferBrief:
    """Предложение и ключевые боли клиента."""

    pains: List[str] = field(default_factory=list)
    value_proposition: str = ""
    call_to_action: str = (
        "Если тема актуальна, буду признателен за коммерческое предложение."
    )


@dataclass
class EmailTemplate:
    """Готовое письмо."""

    subject: str
    body: str


@dataclass
class GeneratedEmail:
    """Результат генерации письма вместе с исходным запросом."""

    template: EmailTemplate
    request_payload: Optional[Dict[str, object]] = None


class EmailGenerationError(RuntimeError):
    """Ошибка генерации письма через LLM."""


class EmailGenerator:
    """Инкапсулирует обращение к LLM и fallback-шаблон."""

    def __init__(
        self,
        *,
        model: str | None = None,
        language: str = "ru",
        temperature: float = 0.4,
        timeout: float | None = None,
    ) -> None:
        self.settings = get_settings()
        self.model = model or self.settings.email_generation_llm_model
        self.language = language
        self.temperature = temperature
        self.timeout = timeout or self.settings.email_generation_llm_timeout_seconds
        self._prompt_templates = self._load_prompt_templates()

    def generate(
        self,
        company: CompanyBrief,
        offer: OfferBrief,
        contact: Optional[ContactBrief] = None,
    ) -> GeneratedEmail:
        """Возвращает готовый шаблон и исходный запрос к LLM."""
        payload: Optional[Dict[str, object]] = None
        if not self._llm_available():
            LOGGER.warning(
                "LLM для генерации писем не настроен, используется fallback-шаблон."
            )
            raise EmailGenerationError("EMAIL_GENERATION_LLM is not configured")

        try:
            payload = self._build_payload(company, offer, contact)
            response = self._request_openai(payload)
            parsed = self._parse_openai_response(response)
            if parsed:
                return GeneratedEmail(template=parsed, request_payload=payload)
            raise EmailGenerationError("LLM returned empty or invalid email payload")
        except httpx.HTTPError as exc:  # noqa: PERF203
            LOGGER.error("Ошибка обращения к OpenAI: %s", exc)
            raise EmailGenerationError(f"LLM request failed: {exc}") from exc

    def _build_payload(
        self,
        company: CompanyBrief,
        offer: OfferBrief,
        contact: Optional[ContactBrief],
    ) -> Dict[str, object]:
        prompt_key = self._prompt_key(company.entity_type)
        payload = {
            "model": self.model,
            "reasoning": {"effort": self.settings.email_generation_llm_reasoning_effort},
            "text": {"format": {"type": "json_schema", **self._response_schema()}},
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": self._system_prompt_text(prompt_key),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": self._user_prompt_text(prompt_key),
                        }
                    ],
                },
            ],
        }
        if not self._should_omit_temperature():
            payload["temperature"] = self.temperature
        return payload

    def _should_omit_temperature(self) -> bool:
        return self.model.startswith("gpt-5")

    def _request_openai(self, payload: Dict[str, object]) -> Dict[str, object]:
        LOGGER.debug("Запрос к LLM: %s", payload)

        if self.settings.email_generation_llm_provider == "gateway":
            gateway_url = (self.settings.email_generation_llm_gateway_url or "").rstrip("/")
            if not gateway_url:
                raise httpx.HTTPError("EMAIL_GENERATION_LLM_GATEWAY_URL is not configured")
            headers = {"Content-Type": "application/json"}
            if self.settings.email_generation_llm_gateway_api_key:
                headers["Authorization"] = (
                    f"Bearer {self.settings.email_generation_llm_gateway_api_key}"
                )
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    f"{gateway_url}{EMAIL_GENERATION_GATEWAY_PATH}",
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                return response.json()

        headers = {
            "Authorization": f"Bearer {self.settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(OPENAI_RESPONSES_URL, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()

    def _llm_available(self) -> bool:
        if self.settings.email_generation_llm_provider == "gateway":
            return bool(self.settings.email_generation_llm_gateway_url)
        return bool(self.settings.openai_api_key)

    def _parse_openai_response(self, response: Dict[str, object]) -> Optional[EmailTemplate]:
        try:
            content = _extract_responses_output_text(response)
            if not content:
                return None
            parsed = json.loads(content)
            return EmailTemplate(subject=parsed["subject"], body=parsed["body"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            LOGGER.error("Не удалось интерпретировать ответ LLM: %s", response)
            return None

    def _load_prompt_templates(self) -> Dict[str, Dict[str, Template]]:
        templates: Dict[str, Dict[str, Template]] = {}
        for key, paths in PROMPT_PATHS.items():
            templates[key] = {
                "system": Template(paths["system"].read_text(encoding="utf-8").strip()),
                "user": Template(paths["user"].read_text(encoding="utf-8").strip()),
            }
        return templates

    def _prompt_key(self, entity_type: Optional[str]) -> str:
        if entity_type == "real_estate_agency":
            return "real_estate_agency"
        return "mall"

    def _system_prompt_text(self, prompt_key: str) -> str:
        return self._prompt_templates[prompt_key]["system"].substitute(language=self.language)

    def _user_prompt_text(self, prompt_key: str) -> str:
        return self._prompt_templates[prompt_key]["user"].substitute(language=self.language)

    def _fallback_template(
        self,
        company: CompanyBrief,
        offer: OfferBrief,
        contact: Optional[ContactBrief],
    ) -> EmailTemplate:
        subject = self._fallback_subject(company.entity_type)
        segment_name = self._segment_name(company.entity_type)
        sender_profile = self._sender_profile(company.entity_type)
        if company.entity_type == "mall":
            observation = self._observation_line(company)
            request_line = (
                "Рассматриваем ваш город для открытия магазина и хотели бы понять, "
                "есть ли у вас подходящие площади для размещения."
            )
            cta_line = (
                "Если тема актуальна, буду признателен, если направите условия аренды, "
                "доступные площади или коммерческое предложение."
            )
        elif company.entity_type == "real_estate_agency":
            observation = self._observation_line(company)
            request_line = (
                "Ищем помещение под магазин в вашем городе и хотели бы узнать, "
                "какие объекты вы могли бы предложить под такой запрос."
            )
            cta_line = (
                "Если у вас есть релевантные варианты, буду признателен за подборку "
                "или коммерческое предложение."
            )
        else:
            observation = self._observation_line(company)
            request_line = (
                "Сейчас рассматриваем ваш город и ищем помещение для аренды под магазин."
            )
            cta_line = offer.call_to_action
        body_lines = [
            "Добрый день!",
            sender_profile,
            f"Посмотрел ваш сайт — вижу, что вы работаете как {segment_name}.",
            observation,
            request_line,
            cta_line,
            "С уважением,",
            "Марк",
        ]
        body = "\n".join(body_lines)
        return EmailTemplate(subject=subject, body=body)

    def _fallback_subject(self, entity_type: Optional[str]) -> str:
        if entity_type == "mall":
            return "Запрос по аренде помещения в вашем ТЦ"
        if entity_type == "real_estate_agency":
            return "Запрос по помещениям в вашем городе"
        return "Запрос по аренде помещения"

    def _sender_profile(self, entity_type: Optional[str]) -> str:
        if entity_type == "mall":
            return (
                "Представляю крупную розничную сеть по продаже алкогольной продукции, "
                "сейчас смотрим локации в вашем городе."
            )
        if entity_type == "real_estate_agency":
            return (
                "Представляю крупную розничную сеть по продаже алкогольной продукции, "
                "подбираем помещения в вашем городе под открытие магазина."
            )
        return (
            "Представляю крупную розничную сеть по продаже алкогольной продукции, "
            "сейчас рассматриваем помещения в вашем городе."
        )

    def _observation_line(self, company: CompanyBrief) -> str:
        excerpt = " ".join(company.highlights).strip() if company.highlights else ""
        if excerpt:
            return "Посмотрел ваш сайт и краткое описание объектов."
        return "Посмотрел ваш сайт и решил написать напрямую."

    def _outreach_goal(self, entity_type: Optional[str]) -> Dict[str, str]:
        if entity_type == "mall":
            return {
                "target": "mall",
                "ask": (
                    "Запросить условия аренды, доступные площади и коммерческое предложение "
                    "по размещению в ТЦ."
                ),
            }
        if entity_type == "real_estate_agency":
            return {
                "target": "agency",
                "ask": (
                    "Запросить релевантные объекты под аренду магазина и коммерческое предложение."
                ),
            }
        return {
            "target": "generic",
            "ask": "Запросить подходящие помещения для аренды в городе.",
        }

    @staticmethod
    def _segment_name(entity_type: Optional[str]) -> str:
        if entity_type == "mall":
            return "торговый центр"
        if entity_type == "real_estate_agency":
            return "агентство недвижимости"
        return "компания"

    def _response_schema(self) -> Dict[str, object]:
        return {
            "name": "EmailTemplate",
            "schema": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string"},
                    "body": {"type": "string"},
                },
                "required": ["subject", "body"],
                "additionalProperties": False,
            },
        }
