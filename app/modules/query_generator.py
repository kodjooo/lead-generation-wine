"""Генерация поисковых запросов по списку городов и типам организаций."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Callable, List, Optional
from zoneinfo import ZoneInfo

from app.modules.constants import EXCLUDED_DOMAINS

DEFAULT_CONFIG = {
    "language": "ru",
    "night_window": {"start_local": "00:00", "end_local": "07:59", "timezone": "Europe/Moscow"},
    "spacing_seconds": 45,
    "region_fallback_lr": 225,
    "excluded_domains": sorted(EXCLUDED_DOMAINS),
    "entity_queries": {
        "mall": [
            {
                "query": "{city} торговый центр официальный сайт",
                "trigger": "shopping_mall_official_site",
            },
            {
                "query": "{city} трц официальный сайт",
                "trigger": "shopping_mall_trc_official_site",
            },
        ],
        "real_estate_agency": [
            {
                "query": "{city} агентство недвижимости официальный сайт",
                "trigger": "real_estate_agency_official_site",
            },
            {
                "query": "{city} риэлторское агентство официальный сайт",
                "trigger": "real_estate_agency_realtor_official_site",
            },
        ],
    },
    "regions_lr": {
        "россия": 225,
        "москва и московская область": 1,
        "москва": 213,
        "санкт‑петербург": 2,
        "saint petersburg": 2,
        "архангельск": 20,
        "назрань": 1092,
        "астрахань": 37,
        "нальчик": 30,
        "барнаул": 197,
        "нижний новгород": 47,
        "белгород": 4,
        "новосибирск": 65,
        "благовещенск": 77,
        "омск": 66,
        "брянск": 191,
        "орёл": 10,
        "орел": 10,
        "великий новгород": 24,
        "оренбург": 48,
        "владивосток": 75,
        "пенза": 49,
        "владикавказ": 33,
        "пермь": 50,
        "владимир": 192,
        "псков": 25,
        "волгоград": 38,
        "ростов-на-дону": 39,
        "вологда": 21,
        "рязань": 11,
        "воронеж": 193,
        "самара": 51,
        "грозный": 1106,
        "екатеринбург": 54,
        "саранск": 42,
        "иваново": 5,
        "смоленск": 12,
        "иркутск": 63,
        "сочи": 239,
        "йошкар-ола": 41,
        "ставрополь": 36,
        "казань": 43,
        "сургут": 973,
        "калининград": 22,
        "тамбов": 13,
        "кемерово": 64,
        "тверь": 14,
        "кострома": 7,
        "томск": 67,
        "краснодар": 35,
        "тула": 15,
        "красноярск": 62,
        "ульяновск": 195,
        "курган": 53,
        "уфа": 172,
        "курск": 8,
        "хабаровск": 76,
        "липецк": 9,
        "чебоксары": 45,
        "махачкала": 28,
        "челябинск": 56,
        "черкесск": 1104,
        "ярославль": 16,
        "мурманск": 23,
    },
}


@dataclass
class CityRow:
    """Исходные данные строки Google Sheets."""

    row_index: int
    city: str
    country: Optional[str]
    batch_tag: Optional[str]
    enabled_malls: bool = True
    enabled_agencies: bool = True


@dataclass
class GeneratedQuery:
    """Результат генерации одного поискового запроса."""

    query_text: str
    query_hash: str
    region_code: int
    scheduled_for: datetime
    trigger: str
    metadata: dict


class QueryGenerator:
    """Формирует поисковые запросы для ТЦ и агентств недвижимости."""

    def __init__(
        self,
        config: dict | None = None,
        *,
        now_func: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.config = config or DEFAULT_CONFIG
        self._now_func = now_func
        self._spacing = int(self.config.get("spacing_seconds", 45))
        self._language = self.config.get("language", "ru")
        night_cfg = self.config.get("night_window", {})
        self._night_tz = ZoneInfo(night_cfg.get("timezone", "UTC"))
        self._window_start_local = self._parse_time(night_cfg.get("start_local", "00:00"))
        self._window_end_local = self._parse_time(night_cfg.get("end_local", "07:59"))
        self._regions_map = {
            self._normalize_key(key): value for key, value in self.config.get("regions_lr", {}).items()
        }
        self._region_fallback = int(self.config.get("region_fallback_lr", 225))
        self._entity_queries = self.config.get("entity_queries", {})
        self._excluded_domains = tuple(sorted(self.config.get("excluded_domains", [])))

    @staticmethod
    def _parse_time(value: str) -> time:
        hours, minutes = value.split(":", 1)
        return time(int(hours), int(minutes))

    @staticmethod
    def _normalize_key(value: str | None) -> str:
        return (value or "").strip().lower()

    def _resolve_region(self, city: Optional[str], country: Optional[str]) -> int:
        city_key = self._normalize_key(city)
        if city_key and city_key in self._regions_map:
            return self._regions_map[city_key]
        country_key = self._normalize_key(country)
        if country_key and country_key in self._regions_map:
            return self._regions_map[country_key]
        return self._region_fallback

    def _window_bounds(self, reference_date) -> tuple[datetime, timedelta]:
        start_local = datetime.combine(reference_date, self._window_start_local, self._night_tz)
        end_local = datetime.combine(reference_date, self._window_end_local, self._night_tz)
        if self._window_end_local <= self._window_start_local:
            end_local += timedelta(days=1)
        duration = end_local - start_local
        return start_local.astimezone(timezone.utc), duration

    def _next_window_start(self, now: datetime) -> tuple[datetime, datetime]:
        start_today, duration = self._window_bounds(now.date())
        if self._window_end_local <= self._window_start_local and now < start_today:
            start_prev = start_today - timedelta(days=1)
            end_prev = start_prev + duration
            if start_prev <= now <= end_prev:
                return now, end_prev

        end_today = start_today + duration
        if start_today <= now <= end_today:
            return now, end_today
        if now < start_today:
            return start_today, end_today

        start_next = start_today + timedelta(days=1)
        end_next = start_next + duration
        return start_next, end_next

    def _build_queries(self, row: CityRow) -> List[tuple[str, str, str]]:
        queries: List[tuple[str, str, str]] = []
        city = row.city.strip()
        if not city:
            return queries

        if row.enabled_malls:
            for item in self._entity_queries.get("mall", []):
                queries.append((item["query"].format(city=city), item["trigger"], "mall"))
        if row.enabled_agencies:
            for item in self._entity_queries.get("real_estate_agency", []):
                queries.append(
                    (
                        item["query"].format(city=city),
                        item["trigger"],
                        "real_estate_agency",
                    )
                )
        return queries

    def generate(self, row: CityRow) -> List[GeneratedQuery]:
        """Формирует список запросов для строки листа."""
        queries = self._build_queries(row)
        if not queries:
            return []

        now = self._now_func()
        window_start, window_end = self._next_window_start(now)
        region_code = self._resolve_region(row.city, row.country)

        result: List[GeneratedQuery] = []
        for index, (query_text, trigger, entity_type) in enumerate(queries):
            scheduled_for = window_start + timedelta(seconds=self._spacing * index)
            if scheduled_for > window_end:
                break
            cleaned_query = " ".join(query_text.split())
            query_hash = hashlib.sha1(
                f"{cleaned_query}|{region_code}".encode("utf-8"),
                usedforsecurity=False,
            ).hexdigest()
            metadata = {
                "city": row.city.strip(),
                "country": row.country.strip() if row.country else None,
                "batch_tag": row.batch_tag.strip() if row.batch_tag else None,
                "language": self._language,
                "selection": "strict",
                "entity_type": entity_type,
                "trigger": trigger,
                "excluded_domains": list(self._excluded_domains),
            }
            result.append(
                GeneratedQuery(
                    query_text=cleaned_query,
                    query_hash=query_hash,
                    region_code=region_code,
                    scheduled_for=scheduled_for,
                    trigger=trigger,
                    metadata=metadata,
                )
            )
        return result
