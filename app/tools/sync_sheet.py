"""CLI для синхронизации Google Sheets с очередью запросов по городам."""

from __future__ import annotations

import argparse
import logging

from app.config import get_settings
from app.modules.sheet_sync import build_service


def main() -> None:
    parser = argparse.ArgumentParser(description="Синхронизирует лист городов с БД.")
    parser.add_argument(
        "--batch-tag",
        help="Обработать только строки с указанным batch_tag",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    settings = get_settings()
    service = build_service(settings)
    summary = service.sync(batch_tag=args.batch_tag)
    logging.info(
        "Готово: обработано %s строк, добавлено %s запросов, дубликатов %s, ошибок %s",
        summary.processed_rows,
        summary.inserted_queries,
        summary.duplicate_queries,
        summary.errors,
    )


if __name__ == "__main__":
    main()
