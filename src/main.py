# src/main.py

from src.config import settings
from src.pipeline.orchestrator import Orchestrator
from src.utils.logger import setup_logger


def main() -> None:
    setup_logger()
    orch = Orchestrator()

    seeds = [
        settings.base_url,
    ]

    result = orch.crawl(seeds)
    print(result)


if __name__ == "__main__":
    main()