"""Планировщик deferred-запросов."""

import logging
import time

from app.modules.yandex_deferred import NightWindowViolation
from app.orchestrator import PipelineOrchestrator

LOGGER = logging.getLogger("app.scheduler")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    orchestrator = PipelineOrchestrator()
    LOGGER.info("Планировщик готов к созданию deferred-запросов.")

    try:
        while True:
            try:
                scheduled = orchestrator.schedule_deferred_queries()
                LOGGER.info("Поставлено deferred-запросов: %s", scheduled)
            except NightWindowViolation as exc:
                LOGGER.info("Вне ночного окна: %s", exc)

            time.sleep(orchestrator.config.poll_interval_seconds)
    except KeyboardInterrupt:
        LOGGER.info("Планировщик остановлен пользователем.")


if __name__ == "__main__":
    main()
