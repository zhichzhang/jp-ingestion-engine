# src/crawler/tests/winery_extractor_test.py

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Type

from bs4 import BeautifulSoup

from src.crawler.fetcher import Fetcher
from src.crawler.extractors.winery import (
    WineryDescriptionExtractor,
    WineryFamilySpiritExtractor,
    WineryHistoryTimelineExtractor,
)
from src.crawler.tests.base import ExtractorTestStrategy, ExtractorTestResult


HOME_URL = "https://www.josephperrier.com/en/?v=0b3b97fa6688"
HISTORY_URL = "https://www.josephperrier.com/en/maison/histoire/?v=0b3b97fa6688"
FAMILY_SPIRIT_URL = "https://www.josephperrier.com/en/maison/famille/?v=0b3b97fa6688"


@dataclass(frozen=True)
class WineryTestCase:
    name: str
    url: str
    extractor_cls: Type


class WineryExtractorTest(ExtractorTestStrategy):
    def __init__(self, test_case: WineryTestCase) -> None:
        self.test_case = test_case

    @property
    def name(self) -> str:
        return self.test_case.name

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = self.test_case.extractor_cls()
        result = extractor.extract(soup, url)
        return ExtractorTestResult(
            name=self.name,
            extractor=extractor.name,
            data=result.data,
        )


class ExtractorTester:
    def __init__(self, strategies: Iterable[ExtractorTestStrategy]) -> None:
        self.strategies = list(strategies)

    def run_all(self, soup: BeautifulSoup, url: str) -> list[ExtractorTestResult]:
        return [strategy.run(soup, url) for strategy in self.strategies]

    def print_all(self, soup: BeautifulSoup, url: str) -> None:
        for result in self.run_all(soup, url):
            print(f"\n[{result.name}]")
            print(f"extractor: {result.extractor}")
            print(f"data: {json.dumps(result.data, ensure_ascii=False, indent=2, default=str)}")


def _fetch_soup(url: str) -> BeautifulSoup:
    html, _ = Fetcher().get(url)
    return BeautifulSoup(html, "html.parser")


def main() -> None:
    test_cases = [
        WineryTestCase(
            name="winery_description",
            url=HOME_URL,
            extractor_cls=WineryDescriptionExtractor,
        ),
        WineryTestCase(
            name="history_timeline",
            url=HISTORY_URL,
            extractor_cls=WineryHistoryTimelineExtractor,
        ),
        WineryTestCase(
            name="family_spirit",
            url=FAMILY_SPIRIT_URL,
            extractor_cls=WineryFamilySpiritExtractor,
        ),
    ]

    for case in test_cases:
        print(f"\n===== {case.name} =====")
        print(f"url: {case.url}")

        soup = _fetch_soup(case.url)

        tester = ExtractorTester(
            strategies=[
                WineryExtractorTest(case),
            ]
        )
        tester.print_all(soup, case.url)


if __name__ == "__main__":
    main()