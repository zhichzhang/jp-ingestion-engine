# src/pipeline/orchestrator.py

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urlparse

import logging

from src.config import settings
from src.crawler.fetcher import Fetcher
from src.crawler.frontier import Frontier
from src.crawler.normalize import normalize_url, canonical_page_url
from src.crawler.parser import (
    extract_language_switch_url,
    is_french_page,
    parse_any_page,
)
from src.db.repository import Repository
from src.db.supabase_client import get_client
from src.pipeline.batch_writer import BatchWriter
from src.pipeline.dedupe import MemoryDedupe
from src.pipeline.models import MediaRecord, ParsedPage, ProductRecord, WineryRecord

logger = logging.getLogger("orchestrator")


class Orchestrator:
    def __init__(self) -> None:
        self.fetcher = Fetcher()
        self.repository = Repository(get_client())
        self.writer = BatchWriter(self.repository)
        self.dedupe = MemoryDedupe()
        self._seen_final_urls: set[str] = set()
        self._seen_final_urls_lock = Lock()

    def _should_enqueue(self, url: str) -> bool:
        url = normalize_url(url)
        path = urlparse(url).path.rstrip("/")

        allowed_prefixes = (
            "/en",
            "/en/champagnes",
            "/en/champagnes-et-cuvees",
            "/en/maison",
            "/en/engagements",
            "/en/champagnes-cuvees",
        )

        return any(path == prefix or path.startswith(prefix + "/") for prefix in allowed_prefixes)

    def crawl(self, seed_urls: list[str]) -> dict:
        logger.info("[START] seeds=%d", len(seed_urls))

        seed_winery = WineryRecord(
            name="Joseph Perrier",
            description=None,
            website_url=settings.base_url,
            source_page_url=settings.base_url,
        )
        self.writer.flush([seed_winery], [], [])

        frontier = Frontier(seed_urls)

        pending_wineries: list[WineryRecord] = []
        pending_products: list[ProductRecord] = []
        pending_media: list[MediaRecord] = []

        total_pages = 0

        while True:
            batch = frontier.pop_batch(settings.max_workers)
            if not batch:
                logger.info("[STOP] frontier empty")
                break

            logger.info("[BATCH] size=%d", len(batch))

            with ThreadPoolExecutor(max_workers=settings.max_workers) as ex:
                futures = {ex.submit(self._crawl_one, url): url for url in batch}

                for fut in as_completed(futures):
                    url = futures[fut]
                    try:
                        parsed = fut.result()
                        total_pages += 1
                    except Exception as e:
                        logger.error("[ERROR] %s -> %s", url, e)
                        continue

                    logger.debug(
                        "[RESULT] %s | products=%d media=%d",
                        url,
                        len(parsed.products),
                        len(parsed.media),
                    )

                    if parsed.winery and self.dedupe.keep_winery(parsed.winery):
                        pending_wineries.append(parsed.winery)

                    for p in parsed.products:
                        if self.dedupe.keep_product(p):
                            pending_products.append(p)

                    for m in parsed.media:
                        if self.dedupe.keep_media(m):
                            pending_media.append(m)

                    logger.info("[DISCOVERED COUNT] %d", len(parsed.discovered_urls))

                    for discovered in parsed.discovered_urls:
                        logger.info("[DISCOVERED] %s", discovered)
                        if self._should_enqueue(discovered):
                            frontier.add(discovered)

            if (
                len(pending_wineries) >= settings.batch_size
                or len(pending_products) >= settings.batch_size
                or len(pending_media) >= settings.batch_size
            ):
                logger.info("[FLUSH TRIGGERED]")
                self.writer.flush(pending_wineries, pending_products, pending_media)
                pending_wineries.clear()
                pending_products.clear()
                pending_media.clear()

        if pending_wineries or pending_products or pending_media:
            logger.info("[FINAL FLUSH]")
            self.writer.flush(pending_wineries, pending_products, pending_media)

        logger.info("[DONE] pages=%d", total_pages)
        return {"status": "done", "pages": total_pages}

    def _crawl_one(self, url: str) -> ParsedPage:
        html, final_url = self.fetcher.get(url)
        canonical = canonical_page_url(final_url)

        with self._seen_final_urls_lock:
            if canonical in self._seen_final_urls:
                logger.info("[SKIP FINAL] %s", canonical)
                return ParsedPage(None, [], [], set())
            self._seen_final_urls.add(canonical)

        logger.info("[CRAWL] url=%s final_url=%s", url, final_url)

        if is_french_page(html, final_url):
            logger.info("[LANG DETECT] FR page detected: %s", final_url)

            en_url = extract_language_switch_url(html, final_url, "en")
            if en_url:
                logger.info("[LANG SWITCH] candidate en_url=%s", en_url)

                if normalize_url(en_url) == normalize_url(final_url):
                    logger.warning("[LANG SKIP] en_url same as current (after normalize): %s", en_url)
                    return ParsedPage(None, [], [], set())

                logger.info("[LANG ROUTE] fr -> enqueue en: %s -> %s", final_url, en_url)
                return ParsedPage(None, [], [], {en_url})

            logger.warning("[LANG FAIL] french page without en switcher: %s", final_url)
            return ParsedPage(None, [], [], set())

        logger.info("[LANG DETECT] EN or non-FR page: %s", final_url)
        return parse_any_page(html, final_url)