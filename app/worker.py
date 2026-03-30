"""Фоновый воркер для обогащения контактов и отправки писем."""

import logging
import time

from app.orchestrator import PipelineOrchestrator

LOGGER = logging.getLogger("app.worker")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    orchestrator = PipelineOrchestrator()
    LOGGER.info("Воркер запущен.")

    try:
        while True:
            enriched, queued, sent = orchestrator.run_worker_cycle()
            LOGGER.info("Воркер цикл: enriched=%s, queued=%s, sent=%s", enriched, queued, sent)
            time.sleep(orchestrator.config.poll_interval_seconds)
    except KeyboardInterrupt:
        LOGGER.info("Воркер остановлен пользователем.")


if __name__ == "__main__":
    main()
