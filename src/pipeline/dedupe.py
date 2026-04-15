# src/pipeline/dedupe.py

from __future__ import annotations

import logging

from src.crawler.normalize import normalize_url, normalize_media_url
from src.pipeline.models import WineryRecord, ProductRecord, MediaRecord

logger = logging.getLogger("dedupe")


class MemoryDedupe:
    def __init__(self) -> None:
        self.seen_winery_pages: set[str] = set()
        self.seen_product_urls: set[str] = set()
        self.seen_media_urls: set[str] = set()

    def keep_winery(self, winery: WineryRecord) -> bool:
        key = normalize_url(winery.source_page_url) if winery.source_page_url else ""
        if key in self.seen_winery_pages:
            logger.debug("[DEDUP] skip winery page %s", key)
            return False
        self.seen_winery_pages.add(key)
        return True

    def keep_product(self, product: ProductRecord) -> bool:
        key = f"{normalize_url(product.product_url)}::{normalize_url(product.source_page_url)}"
        if key in self.seen_product_urls:
            return False
        self.seen_product_urls.add(key)
        return True

    def keep_media(self, media: MediaRecord) -> bool:
        key = normalize_media_url(media.url) if media.url else ""
        if key in self.seen_media_urls:
            logger.debug("[DEDUP] skip media %s", key)
            return False
        self.seen_media_urls.add(key)
        return True