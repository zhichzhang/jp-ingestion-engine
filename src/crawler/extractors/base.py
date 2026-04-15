# src/crawler/extractors/base.py

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from bs4 import BeautifulSoup

from src.pipeline.models import MediaRecord


@dataclass
class ExtractionResult:
    data: dict[str, Any] = field(default_factory=dict)
    media: list[MediaRecord] = field(default_factory=list)
    discovered_urls: set[str] = field(default_factory=set)


class BaseExtractor(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def selector(self) -> str:
        raise NotImplementedError

    def can_run(self, soup: BeautifulSoup) -> bool:
        return bool(soup.select_one(self.selector))

    @abstractmethod
    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        raise NotImplementedError