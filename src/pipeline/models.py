# src/pipeline/models.py
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class WineryRecord:
    # identity
    name: str
    website_url: str
    source_page_url: str

    # core description
    description: str | None = None

    # structured content
    family_spirit: dict[str, Any] = field(default_factory=dict)
    history_timeline: list[dict[str, Any]] = field(default_factory=list)

    # db-managed timestamps
    created_at: datetime | None = None
    updated_at: datetime | None = None

    # optional db identity
    id: str | None = None

    def to_db_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "website_url": self.website_url,
            "source_page_url": self.source_page_url,
            "description": self.description,
            "family_spirit": self.family_spirit,
            "history_timeline": self.history_timeline,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class ProductRecord:
    # relation
    winery_id: str | None = None

    # identity
    name: str = ""
    product_url: str = ""
    source_page_url: str = ""

    # core description
    description: str | None = None

    # technical attributes
    dosage_g_per_l: float | None = None
    aging: str | None = None
    operating_temperature: str | None = None
    crus_assembles: str | None = None
    millennium: str | None = None

    # grape composition
    grape_chardonnay_percent: float | None = None
    grape_pinot_noir_percent: float | None = None
    grape_meunier_percent: float | None = None

    # awards / ratings
    awards_and_ratings: list[dict[str, Any]] = field(default_factory=list)

    # document
    data_sheet_url: str | None = None

    # db-managed timestamps
    created_at: datetime | None = None
    updated_at: datetime | None = None

    # optional db identity
    id: str | None = None

    def to_db_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "winery_id": self.winery_id,
            "name": self.name,
            "product_url": self.product_url,
            "source_page_url": self.source_page_url,
            "description": self.description,
            "dosage_g_per_l": self.dosage_g_per_l,
            "aging": self.aging,
            "operating_temperature": self.operating_temperature,
            "crus_assembles": self.crus_assembles,
            "millennium": self.millennium,
            "grape_chardonnay_percent": self.grape_chardonnay_percent,
            "grape_pinot_noir_percent": self.grape_pinot_noir_percent,
            "grape_meunier_percent": self.grape_meunier_percent,
            "awards_and_ratings": self.awards_and_ratings,
            "data_sheet_url": self.data_sheet_url,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class MediaRecord:
    media_type: str
    url: str
    source_page_url: str

    id: str | None = None
    created_at: datetime | None = None

    def to_db_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "media_type": self.media_type,
            "url": self.url,
            "source_page_url": self.source_page_url,
            "created_at": self.created_at,
        }


@dataclass(slots=True)
class ParsedPage:
    winery: WineryRecord | None = None
    products: list[ProductRecord] = field(default_factory=list)
    media: list[MediaRecord] = field(default_factory=list)
    discovered_urls: set[str] = field(default_factory=set)


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()

        self.total_requests = 0
        self.success_requests = 0
        self.failed_requests = 0

        self.total_seen_urls = 0
        self.unique_urls = 0
        self.duplicate_filtered = 0

        self.batch_flushes = 0
        self.written_wineries = 0
        self.written_products = 0
        self.written_media = 0

        self.latencies = deque(maxlen=10000)
        self.crawled_urls = set()

    def inc(self, name, v=1):
        with self.lock:
            setattr(self, name, getattr(self, name) + v)

    def add_latency(self, t):
        with self.lock:
            self.latencies.append(t)

    def add_crawled(self, url):
        with self.lock:
            self.crawled_urls.add(url)
metrics = Metrics()