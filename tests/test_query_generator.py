"""Тесты генератора запросов по городам."""

from datetime import datetime, timezone

from app.modules.query_generator import CityRow, QueryGenerator


def test_query_generator_builds_queries_for_two_entity_types() -> None:
    generator = QueryGenerator(now_func=lambda: datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc))
    row = CityRow(row_index=2, city="Москва", country="Россия", batch_tag="batch-1")

    queries = generator.generate(row)

    assert len(queries) == 4
    assert queries[0].query_text == "Москва торговый центр официальный сайт"
    assert queries[0].metadata["entity_type"] == "mall"
    assert queries[2].metadata["entity_type"] == "real_estate_agency"
    assert queries[0].region_code == 213
    assert queries[0].scheduled_for == datetime(2025, 1, 1, 21, 0, tzinfo=timezone.utc)
    assert queries[1].scheduled_for == datetime(2025, 1, 1, 21, 0, 45, tzinfo=timezone.utc)


def test_query_generator_fallback_region_and_flags() -> None:
    generator = QueryGenerator(now_func=lambda: datetime(2025, 1, 2, 3, 0, tzinfo=timezone.utc))
    row = CityRow(
        row_index=3,
        city="Неизвестный город",
        country="Казахстан",
        batch_tag=None,
        enabled_malls=False,
        enabled_agencies=True,
    )

    queries = generator.generate(row)

    assert len(queries) == 2
    assert all(query.metadata["entity_type"] == "real_estate_agency" for query in queries)
    assert queries[0].region_code == 225
    assert queries[0].scheduled_for == datetime(2025, 1, 2, 3, 0, tzinfo=timezone.utc)
