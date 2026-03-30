"""Р“РµРЅРµСЂР°С†РёСЏ РїРµСЂСЃРѕРЅР°Р»РёР·РёСЂРѕРІР°РЅРЅС‹С… РїРёСЃРµРј СЃ РїРѕРјРѕС‰СЊСЋ LLM."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import httpx

from app.config import get_settings

LOGGER = logging.getLogger("app.generate_email")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


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
    """РњРёРЅРёРјР°Р»СЊРЅРѕРµ РѕРїРёСЃР°РЅРёРµ РєРѕРјРїР°РЅРёРё РґР»СЏ РїРёСЃСЊРјР°."""

    domain: str
    name: Optional[str] = None
    entity_type: Optional[str] = None
    industry: Optional[str] = None
    highlights: List[str] = field(default_factory=list)


@dataclass
class ContactBrief:
    """РРЅС„РѕСЂРјР°С†РёСЏ Рѕ РєРѕРЅС‚Р°РєС‚РЅРѕРј Р»РёС†Рµ."""

    name: Optional[str] = None
    role: Optional[str] = None
    emails: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)


@dataclass
class OfferBrief:
    """РџСЂРµРґР»РѕР¶РµРЅРёРµ Рё РєР»СЋС‡РµРІС‹Рµ Р±РѕР»Рё РєР»РёРµРЅС‚Р°."""

    pains: List[str] = field(default_factory=list)
    value_proposition: str = ""
    call_to_action: str = "Р”Р°РІР°Р№С‚Рµ РѕР±СЃСѓРґРёРј РІРѕР·РјРѕР¶РЅРѕСЃС‚Рё СЃРѕС‚СЂСѓРґРЅРёС‡РµСЃС‚РІР° РЅР° РєРѕСЂРѕС‚РєРѕРј СЃРѕР·РІРѕРЅРµ."  # noqa: E501


@dataclass
class EmailTemplate:
    """Р“РѕС‚РѕРІРѕРµ РїРёСЃСЊРјРѕ."""

    subject: str
    body: str


@dataclass
class GeneratedEmail:
    """Р РµР·СѓР»СЊС‚Р°С‚ РіРµРЅРµСЂР°С†РёРё РїРёСЃСЊРјР° РІРјРµСЃС‚Рµ СЃ РёСЃС…РѕРґРЅС‹Рј Р·Р°РїСЂРѕСЃРѕРј."""

    template: EmailTemplate
    request_payload: Optional[Dict[str, object]] = None
    used_fallback: bool = False


class EmailGenerator:
    """РРЅРєР°РїСЃСѓР»РёСЂСѓРµС‚ РѕР±СЂР°С‰РµРЅРёРµ Рє LLM Рё fallback-С€Р°Р±Р»РѕРЅ."""

    def __init__(
        self,
        *,
        model: str = "gpt-4.1-mini",
        language: str = "ru",
        temperature: float = 0.4,
        timeout: float = 15.0,
    ) -> None:
        self.model = model
        self.language = language
        self.temperature = temperature
        self.timeout = timeout
        self.settings = get_settings()

    def generate(
        self,
        company: CompanyBrief,
        offer: OfferBrief,
        contact: Optional[ContactBrief] = None,
    ) -> GeneratedEmail:
        """Р’РѕР·РІСЂР°С‰Р°РµС‚ РіРѕС‚РѕРІС‹Р№ С€Р°Р±Р»РѕРЅ Рё РёСЃС…РѕРґРЅС‹Р№ Р·Р°РїСЂРѕСЃ Рє LLM."""
        payload: Optional[Dict[str, object]] = None
        if not self.settings.openai_api_key:
            LOGGER.warning("OPENAI_API_KEY РЅРµ Р·Р°РґР°РЅ, РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ fallback-С€Р°Р±Р»РѕРЅ.")
            template = self._fallback_template(company, offer, contact)
            return GeneratedEmail(template=template, request_payload=None, used_fallback=True)

        try:
            payload = self._build_payload(company, offer, contact)
            response = self._request_openai(payload)
            parsed = self._parse_openai_response(response)
            if parsed:
                return GeneratedEmail(template=parsed, request_payload=payload, used_fallback=False)
            template = self._fallback_template(company, offer, contact)
            return GeneratedEmail(template=template, request_payload=payload, used_fallback=True)
        except httpx.HTTPError as exc:  # noqa: PERF203
            LOGGER.error("РћС€РёР±РєР° РѕР±СЂР°С‰РµРЅРёСЏ Рє OpenAI: %s", exc)
            template = self._fallback_template(company, offer, contact)
            return GeneratedEmail(template=template, request_payload=payload, used_fallback=True)

    def _build_payload(
        self,
        company: CompanyBrief,
        offer: OfferBrief,
        contact: Optional[ContactBrief],
    ) -> Dict[str, object]:
        homepage_excerpt = " ".join(company.highlights) if company.highlights else None
        segment_name = self._segment_name(company.entity_type)
        user_payload = {
            "company": {
                "entity_type": company.entity_type,
                "segment_name": segment_name,
                "homepage_excerpt": homepage_excerpt,
            },
            "guidelines": {
                "language": self.language,
                "avoid_marketing": True,
            },
        }
        return {
            "model": self.model,
            "temperature": self.temperature,
            "text": {"format": {"type": "json_schema", **self._response_schema()}},
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "РўС‹ РњР°СЂРє РђР±РѕСЂС‡Рё, СЃРїРµС†РёР°Р»РёСЃС‚ РїРѕ AI-Р°РІС‚РѕРјР°С‚РёР·Р°С†РёРё. РўРІРѕСЏ Р·Р°РґР°С‡Р° вЂ” РїРёСЃР°С‚СЊ "
                                "РїРµСЂСЃРѕРЅР°Р»РёР·РёСЂРѕРІР°РЅРЅС‹Рµ, С‡РµР»РѕРІРµС‡РµСЃРєРёРµ РїРёСЃСЊРјР° РЅР° СЂСѓСЃСЃРєРѕРј СЏР·С‹РєРµ РґР»СЏ РєРѕРјРїР°РЅРёР№, "
                                "РєРѕС‚РѕСЂС‹Рј РјРѕР¶РЅРѕ РїРѕРјРѕС‡СЊ Р°РІС‚РѕРјР°С‚РёР·Р°С†РёРµР№ РїСЂРѕС†РµСЃСЃРѕРІ СЃ РїРѕРјРѕС‰СЊСЋ РЅРµР№СЂРѕСЃРµС‚РµР№, Python, make.com РёР»Рё n8n. "
                                "РР·Р±РµРіР°Р№ СЂРµРєР»Р°РјРЅРѕРіРѕ С‚РѕРЅР° Рё РїСЂРµРІРѕСЃС…РѕРґРЅС‹С… СЃС‚РµРїРµРЅРµР№. Р”РµР»Р°Р№ Р°РєС†РµРЅС‚ РЅР° РїРѕР»СЊР·Рµ: СЌРєРѕРЅРѕРјРёСЏ РІСЂРµРјРµРЅРё, "
                                "СЃРѕРєСЂР°С‰РµРЅРёРµ Р·Р°С‚СЂР°С‚, СѓСЃС‚СЂР°РЅРµРЅРёРµ СЂСѓС‚РёРЅС‹, РїРѕРІС‹С€РµРЅРёРµ СЌС„С„РµРєС‚РёРІРЅРѕСЃС‚Рё. Р’СЃРµРіРґР° РёСЃРїРѕР»СЊР·СѓР№ JSON-РѕС‚РІРµС‚ СЃ РїРѕР»СЏРјРё subject Рё body. "
                                "Р•СЃС‚СЊ РґРІРµ РѕСЃРЅРѕРІРЅС‹Рµ Р°СѓРґРёС‚РѕСЂРёРё: С‚РѕСЂРіРѕРІС‹Рµ С†РµРЅС‚СЂС‹ Рё Р°РіРµРЅС‚СЃС‚РІР° РЅРµРґРІРёР¶РёРјРѕСЃС‚Рё. "
                                "РџРѕРґР±РёСЂР°Р№ РЅР°Р±Р»СЋРґРµРЅРёСЏ Рё РїСЂРёРјРµСЂС‹ Р°РІС‚РѕРјР°С‚РёР·Р°С†РёРё РїРѕРґ РєРѕРЅРєСЂРµС‚РЅС‹Р№ С‚РёРї РєРѕРјРїР°РЅРёРё. "
                                "РЎС‚СЂСѓРєС‚СѓСЂР° РїРёСЃСЊРјР° С„РёРєСЃРёСЂРѕРІР°РЅР°: С‚РµРјР° РїРµСЂРµРґР°С‘С‚ РёРґРµСЋ РѕРїС‚РёРјРёР·Р°С†РёРё РїСЂРѕС†РµСЃСЃРѕРІ РєРѕРјРїР°РЅРёРё (РЅР°РїСЂРёРјРµСЂ, 'РРґРµСЏ РїРѕ РѕРїС‚РёРјРёР·Р°С†РёРё РїСЂРѕС†РµСЃСЃРѕРІ РІР°С€РµР№ РєРѕРјРїР°РЅРёРё') Рё С‚РµР»Рѕ СЃРѕСЃС‚РѕРёС‚ РёР· Р±Р»РѕРєРѕРІ:\n"
                                "1) РџСЂРёРІРµС‚СЃС‚РІРёРµ 'Р”РѕР±СЂС‹Р№ РґРµРЅСЊ!'.\n"
                                "2) РљРѕСЂРѕС‚РєРѕРµ РїСЂРµРґСЃС‚Р°РІР»РµРЅРёРµ РњР°СЂРєР° Рё РµРіРѕ РїРѕРґС…РѕРґР° (РЅРµР№СЂРѕСЃРµС‚Рё, Python).\n"
                                "3) РЈРїРѕРјРёРЅР°РЅРёРµ, С‡РµРј Р·Р°РЅРёРјР°РµС‚СЃСЏ РєРѕРјРїР°РЅРёСЏ (РёСЃРїРѕР»СЊР·СѓР№ РїСЂРµРґРѕСЃС‚Р°РІР»РµРЅРЅС‹Р№ С‚РµРєСЃС‚, РЅРµ СѓРїРѕРјРёРЅР°Р№ РЅР°Р·РІР°РЅРёРµ). Р”РѕР±Р°РІСЊ РєРѕСЂРѕС‚РєРѕРµ РЅР°Р±Р»СЋРґРµРЅРёРµ (1 РїСЂРµРґР»РѕР¶РµРЅРёРµ) Рѕ С‡С‘Рј-С‚Рѕ, С‡С‚Рѕ РІС‹РґРµР»СЏРµС‚ РєРѕРјРїР°РЅРёСЋ: С‡С‚Рѕ С‚РµР±СЏ РІРїРµС‡Р°С‚Р»РёР»Рѕ, С‡С‚Рѕ РїРѕРєР°Р·Р°Р»РѕСЃСЊ РёРЅС‚РµСЂРµСЃРЅС‹Рј.\n"
                                "4) РћРїРёСЃР°РЅРёРµ РєРѕРЅРєСЂРµС‚РЅРѕРіРѕ РїСЂРѕС†РµСЃСЃР°, РєРѕС‚РѕСЂС‹Р№ РјРѕР¶РЅРѕ СѓРїСЂРѕСЃС‚РёС‚СЊ СЃ РїРѕРјРѕС‰СЊСЋ AI, Рё РѕР¶РёРґР°РµРјРѕРіРѕ СЌС„С„РµРєС‚Р° (СЃРѕРєСЂР°С‚РёС‚СЊ Р·Р°РґРµСЂР¶РєРё, СѓРјРµРЅСЊС€РёС‚СЊ Р·Р°С‚СЂР°С‚С‹ Рё С‚.Рї.).\n"
                                "5) РџСЂРёРіР»Р°С€РµРЅРёРµ РѕР±СЃСѓРґРёС‚СЊ РїСЂРёРјРµСЂС‹.\n"
                                "6) Р—Р°РІРµСЂС€РµРЅРёРµ: 'РЎ СѓРІР°Р¶РµРЅРёРµРј,' + РёРјСЏ Рё РґРѕР»Р¶РЅРѕСЃС‚СЊ.\n"
                                "РЎС‚СЂСѓРєС‚СѓСЂСѓ СЃРѕС…СЂР°РЅСЏР№, РЅРѕ С„РѕСЂРјСѓР»РёСЂРѕРІРєРё С‚РµРјС‹ Рё С‚РµР»Р° РІР°СЂСЊРёСЂСѓР№, С‡С‚РѕР±С‹ РїРёСЃСЊРјР° РЅРµ СЃРѕРІРїР°РґР°Р»Рё РґРѕСЃР»РѕРІРЅРѕ."
                            ),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": json.dumps(user_payload, ensure_ascii=False),
                        }
                    ],
                },
            ],
        }

    def _request_openai(self, payload: Dict[str, object]) -> Dict[str, object]:
        LOGGER.debug("Р—Р°РїСЂРѕСЃ Рє OpenAI: %s", payload)

        headers = {
            "Authorization": f"Bearer {self.settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(OPENAI_RESPONSES_URL, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()

    def _parse_openai_response(self, response: Dict[str, object]) -> Optional[EmailTemplate]:
        try:
            content = _extract_responses_output_text(response)
            if not content:
                return None
            parsed = json.loads(content)
            return EmailTemplate(subject=parsed["subject"], body=parsed["body"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            LOGGER.error("РќРµ СѓРґР°Р»РѕСЃСЊ РёРЅС‚РµСЂРїСЂРµС‚РёСЂРѕРІР°С‚СЊ РѕС‚РІРµС‚ LLM: %s", response)
            return None

    def _fallback_template(
        self,
        company: CompanyBrief,
        offer: OfferBrief,
        contact: Optional[ContactBrief],
    ) -> EmailTemplate:
        subject = "РРґРµСЏ РїРѕ РѕРїС‚РёРјРёР·Р°С†РёРё РїСЂРѕС†РµСЃСЃРѕРІ РІР°С€РµР№ РєРѕРјРїР°РЅРёРё"
        segment_name = self._segment_name(company.entity_type)
        if company.entity_type == "mall":
            process_hint = (
                "РЅР°РїСЂРёРјРµСЂ, Р°РІС‚РѕРјР°С‚РёР·РёСЂРѕРІР°С‚СЊ РѕР±СЂР°Р±РѕС‚РєСѓ Р·Р°СЏРІРѕРє РЅР° Р°СЂРµРЅРґСѓ, РІС…РѕРґСЏС‰РёС… РѕР±СЂР°С‰РµРЅРёР№ Р°СЂРµРЅРґР°С‚РѕСЂРѕРІ "
                "Рё РїРѕРґРіРѕС‚РѕРІРєСѓ СЃРІРѕРґРѕРє РїРѕ Р·Р°РїРѕР»РЅСЏРµРјРѕСЃС‚Рё, С‡С‚РѕР±С‹ РєРѕРјР°РЅРґР° РјРµРЅСЊС€Рµ С‚СЂР°С‚РёР»Р° РІСЂРµРјРµРЅРё РЅР° СЂСѓС‚РёРЅСѓ"
            )
            observation = "РЎР°Р№С‚ РїСЂРѕРёР·РІРѕРґРёС‚ РІРїРµС‡Р°С‚Р»РµРЅРёРµ РїР»РѕС‰Р°РґРєРё СЃ Р±РѕР»СЊС€РёРј С‡РёСЃР»РѕРј РїР°СЂР°Р»Р»РµР»СЊРЅС‹С… РєРѕРјРјСѓРЅРёРєР°С†РёР№ Рё РїСЂРѕС†РµСЃСЃРѕРІ."
        elif company.entity_type == "real_estate_agency":
            process_hint = (
                "РЅР°РїСЂРёРјРµСЂ, Р°РІС‚РѕРјР°С‚РёР·РёСЂРѕРІР°С‚СЊ РїРµСЂРІРёС‡РЅС‹Р№ СЂР°Р·Р±РѕСЂ РІС…РѕРґСЏС‰РёС… Р·Р°СЏРІРѕРє, РјР°СЂС€СЂСѓС‚РёР·Р°С†РёСЋ Р»РёРґРѕРІ "
                "Рё РїРѕРґРіРѕС‚РѕРІРєСѓ РєР»РёРµРЅС‚СЃРєРёС… РїРѕРґР±РѕСЂРѕРє, С‡С‚РѕР±С‹ РєРѕРјР°РЅРґР° РјРµРЅСЊС€Рµ С‚СЂР°С‚РёР»Р° РІСЂРµРјРµРЅРё РЅР° СЂСѓС‚РёРЅСѓ"
            )
            observation = "РџРѕ СЃР°Р№С‚Сѓ РІРёРґРЅРѕ, С‡С‚Рѕ Сѓ РІР°СЃ РјРЅРѕРіРѕ РѕРґРЅРѕС‚РёРїРЅС‹С… РєРѕРјРјСѓРЅРёРєР°С†РёР№, РіРґРµ Р°РІС‚РѕРјР°С‚РёР·Р°С†РёСЏ РјРѕР¶РµС‚ Р±С‹СЃС‚СЂРѕ РѕРєСѓРїРёС‚СЊСЃСЏ."
        else:
            industry_fragment = company.industry or "РІР°С€РµР№ СЃС„РµСЂРµ"
            if offer.value_proposition:
                automation_example = offer.value_proposition.lower()
                process_hint = (
                    f"РЅР°РїСЂРёРјРµСЂ, {automation_example}, С‡С‚РѕР±С‹ РєРѕРјР°РЅРґР° РјРµРЅСЊС€Рµ С‚СЂР°С‚РёР»Р° РІСЂРµРјРµРЅРё РЅР° СЂСѓС‚РёРЅСѓ"
                )
            elif offer.pains:
                pain_focus = offer.pains[0].lower()
                process_hint = (
                    f"РЅР°РїСЂРёРјРµСЂ, Р°РІС‚РѕРјР°С‚РёР·РёСЂРѕРІР°С‚СЊ С‡Р°СЃС‚Рё РїСЂРѕС†РµСЃСЃР° РІРѕРєСЂСѓРі {pain_focus}, "
                    "С‡С‚РѕР±С‹ РєРѕРјР°РЅРґР° РјРµРЅСЊС€Рµ С‚СЂР°С‚РёР»Р° РІСЂРµРјРµРЅРё РЅР° СЂСѓС‚РёРЅСѓ"
                )
            else:
                process_hint = (
                    "РЅР°РїСЂРёРјРµСЂ, Р°РІС‚РѕРјР°С‚РёР·РёСЂРѕРІР°С‚СЊ РѕР±СЂР°Р±РѕС‚РєСѓ Р·Р°СЏРІРѕРє РёР»Рё РїРѕРґРіРѕС‚РѕРІРєСѓ РѕС‚С‡С‘С‚РѕРІ, "
                    "С‡С‚РѕР±С‹ РєРѕРјР°РЅРґР° РјРµРЅСЊС€Рµ С‚СЂР°С‚РёР»Р° РІСЂРµРјРµРЅРё РЅР° СЂСѓС‚РёРЅСѓ"
                )
            observation = f"РџРѕ СЃР°Р№С‚Сѓ РІРёРґРЅРѕ, С‡С‚Рѕ РІС‹ СЃРёСЃС‚РµРјРЅРѕ СЂР°Р·РІРёРІР°РµС‚Рµ РїСЂРѕС†РµСЃСЃС‹ РІ СЃС„РµСЂРµ {industry_fragment}."
        body_lines = [
            "Р”РѕР±СЂС‹Р№ РґРµРЅСЊ!",
            "РњРµРЅСЏ Р·РѕРІСѓС‚ РњР°СЂРє, СЏ Р·Р°РЅРёРјР°СЋСЃСЊ Р°РІС‚РѕРјР°С‚РёР·Р°С†РёРµР№ Р±РёР·РЅРµСЃ-РїСЂРѕС†РµСЃСЃРѕРІ СЃ РїРѕРјРѕС‰СЊСЋ РЅРµР№СЂРѕСЃРµС‚РµР№ Рё Python.",
            f"РџРѕСЃРјРѕС‚СЂРµР» РІР°С€ СЃР°Р№С‚ вЂ” РїРѕ РѕРїРёСЃР°РЅРёСЋ РІРёРґРЅРѕ, С‡С‚Рѕ РІС‹ СЂР°Р±РѕС‚Р°РµС‚Рµ РєР°Рє {segment_name}.",
            observation,
            f"РњРЅРµ РєР°Р¶РµС‚СЃСЏ, Р·РґРµСЃСЊ РјРѕР¶РЅРѕ СѓРїСЂРѕСЃС‚РёС‚СЊ РїСЂРѕС†РµСЃСЃС‹, {process_hint}.",
            "",
            "Р•СЃР»Рё РёРЅС‚РµСЂРµСЃРЅРѕ, РјРѕРіСѓ РїРѕРєР°Р·Р°С‚СЊ РЅР° РєРѕРЅРєСЂРµС‚РЅС‹С… РїСЂРёРјРµСЂР°С…, РєР°Рє СЌС‚Рѕ СЂР°Р±РѕС‚Р°РµС‚.",
            "",
            "РЎ СѓРІР°Р¶РµРЅРёРµРј,",
            "РњР°СЂРє РђР±РѕСЂС‡Рё",
            "AI-Automation Specialist",
        ]
        body = "\n".join(body_lines)
        return EmailTemplate(subject=subject, body=body)

    @staticmethod
    def _segment_name(entity_type: Optional[str]) -> str:
        if entity_type == "mall":
            return "С‚РѕСЂРіРѕРІС‹Р№ С†РµРЅС‚СЂ"
        if entity_type == "real_estate_agency":
            return "Р°РіРµРЅС‚СЃС‚РІРѕ РЅРµРґРІРёР¶РёРјРѕСЃС‚Рё"
        return "РєРѕРјРїР°РЅРёСЏ"

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
            },
        }

