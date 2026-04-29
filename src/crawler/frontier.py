# src/crawler/frontier.py

from collections import deque
import logging

from src.crawler.normalize import normalize_url
from src.pipeline.models import metrics

logger = logging.getLogger("frontier")


class Frontier:
    def __init__(self, seeds: list[str]) -> None:
        self.queue = deque()
        self.seen: set[str] = set()
        self.queued: set[str] = set()

        logger.info(f"[INIT] seeds={len(seeds)}")

        for url in seeds:
            self.add(url)

    # def add(self, url: str) -> None:
    #     if not url:
    #         return
    #
    #     norm = normalize_url(url)
    #
    #     if norm in self.seen:
    #         logger.debug(f"[SKIP] already seen: {norm}")
    #         return
    #
    #     if norm in self.queued:
    #         logger.debug(f"[SKIP] already queued: {norm}")
    #         return
    #
    #     self.queue.append(url)
    #     self.queued.add(norm)
    #
    #     logger.debug(f"[ADD] raw={url} norm={norm}")

    def add(self, url: str) -> None:
        if not url:
            return

        norm = normalize_url(url)

        metrics.inc("total_seen_urls")

        if norm in self.seen:
            metrics.inc("duplicate_filtered")
            return

        if norm in self.queued:
            metrics.inc("duplicate_filtered")
            return

        self.queue.append(url)
        self.queued.add(norm)

        metrics.inc("unique_urls")

        logger.debug(f"[ADD] raw={url} norm={norm}")

    def add_many(self, urls: set[str]) -> None:
        added = 0

        for url in urls:
            before = len(self.queue)
            self.add(url)
            if len(self.queue) > before:
                added += 1

        logger.info(f"[ADD_MANY] added={added} total_queue={len(self.queue)}")

    def pop_batch(self, batch_size: int) -> list[str]:
        batch: list[str] = []

        while self.queue and len(batch) < batch_size:
            url = self.queue.popleft()
            norm = normalize_url(url)

            self.queued.discard(norm)

            if norm in self.seen:
                continue

            self.seen.add(norm)
            batch.append(url)

        if batch:
            logger.info(
                f"[POP] batch_size={len(batch)} "
                f"remaining_queue={len(self.queue)} "
                f"seen={len(self.seen)}"
            )

        return batch