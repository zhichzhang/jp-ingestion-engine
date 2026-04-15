# src/crawler/tests/language_swicher_extractor_test.py

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

from bs4 import BeautifulSoup

from src.crawler.fetcher import Fetcher
from src.crawler.extractors.general import LanguageSwitchUrlExtractor
from src.crawler.tests.base import ExtractorTestStrategy, ExtractorTestResult


class LanguageSwitcherExtractorTest(ExtractorTestStrategy):
    def __init__(self, target_lang: str) -> None:
        self.target_lang = target_lang.lower().strip()

    @property
    def name(self) -> str:
        return f"language_switch_{self.target_lang}"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = LanguageSwitchUrlExtractor(target_lang=self.target_lang)
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


def main() -> None:
    url = "https://www.josephperrier.com/"
    html, _ = Fetcher().get(url)
    soup = BeautifulSoup(html, "html.parser")

    tester = ExtractorTester(
        strategies=[
            LanguageSwitcherExtractorTest("en"),
            LanguageSwitcherExtractorTest("fr"),
        ]
    )

    tester.print_all(soup, url)


if __name__ == "__main__":
    main()