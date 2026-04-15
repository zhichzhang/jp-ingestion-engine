from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Any

from bs4 import BeautifulSoup


@dataclass(frozen=True)
class ExtractorTestResult:
    name: str
    extractor: str
    data: dict[str, Any]


class ExtractorTestStrategy(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        raise NotImplementedError