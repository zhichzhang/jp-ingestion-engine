# src/crawler/tests/media_extractor_test.py

from __future__ import annotations

import json
from typing import Iterable

from bs4 import BeautifulSoup

from src.crawler.extractors.general import MediaExtractor
from src.crawler.fetcher import Fetcher
from src.crawler.tests.base import ExtractorTestStrategy, ExtractorTestResult


class MediaExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "media"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = MediaExtractor()
        result = extractor.extract(soup, url)

        image_count = sum(1 for m in result.media if m.media_type == "image")
        video_count = sum(1 for m in result.media if m.media_type == "video")

        media_samples = [
            {
                "media_type": m.media_type,
                "url": m.url,
                "source_page_url": m.source_page_url,
            }
            for m in result.media[:-1]
        ]

        return ExtractorTestResult(
            name=self.name,
            extractor=extractor.name,
            data={
                "media_count": len(result.media),
                "image_count": image_count,
                "video_count": video_count,
                "media_samples": media_samples,
            },
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
            print(
                f"data: {json.dumps(result.data, ensure_ascii=False, indent=2, default=str)}"
            )


def main() -> None:
    url = "https://www.josephperrier.com/en/?v=0b3b97fa6688"
    html, _ = Fetcher().get(url)
    soup = BeautifulSoup(html, "html.parser")

    tester = ExtractorTester(
        strategies=[
            MediaExtractorTest(),
        ]
    )

    tester.print_all(soup, url)


if __name__ == "__main__":
    main()