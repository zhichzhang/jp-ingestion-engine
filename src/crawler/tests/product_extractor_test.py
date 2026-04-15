# src/crawler/extractors/product_extractor_test.py

from __future__ import annotations

import json
from typing import Any, Iterable

from bs4 import BeautifulSoup

from src.crawler.fetcher import Fetcher
from src.crawler.extractors.product import (
    AwardsExtractor,
    DataSheetExtractor,
    GrapeCompositionExtractor,
    InternalLinksExtractor,
    ProductDescriptionExtractor,
    ProductNameExtractor,
    RightTechKVExtractor,
    TemperatureExtractor,
    TastingExtractor,
)
from src.crawler.tests.base import ExtractorTestResult, ExtractorTestStrategy
from src.pipeline.models import MediaRecord


def _media_to_dict(media: MediaRecord) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in (
        "media_type",
        "url",
        "source_page_url",
        "entity_type",
        "entity_key",
        "alt_text",
        "position",
    ):
        if hasattr(media, field):
            out[field] = getattr(media, field)
    return out


def _build_result(
    *,
    name: str,
    extractor_name: str,
    extracted_data: dict[str, Any],
    media: list[MediaRecord],
    discovered_urls: set[str],
    media_sample_limit: int = 3,
    discovered_urls_sample_limit: int = 3,
) -> ExtractorTestResult:
    return ExtractorTestResult(
        name=name,
        extractor=extractor_name,
        data={
            "extracted": extracted_data,
            "meta": {
                "media_count": len(media),
                "discovered_urls_count": len(discovered_urls),
                "media_samples": [_media_to_dict(m) for m in media[:media_sample_limit]],
                "discovered_urls_samples": sorted(list(discovered_urls))[:discovered_urls_sample_limit],
            },
        },
    )


class ProductNameExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "product_name"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = ProductNameExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class ProductDescriptionExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "product_description"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = ProductDescriptionExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class GrapeCompositionExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "grape_composition"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = GrapeCompositionExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class TemperatureExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "operating_temperature"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = TemperatureExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class RightTechKVExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "right_tech_kv"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = RightTechKVExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class DataSheetExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "data_sheet"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = DataSheetExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class AwardsExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "awards_and_ratings"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = AwardsExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
        )


class InternalLinksExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "internal_links"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = InternalLinksExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
            media_sample_limit=3,
            discovered_urls_sample_limit=10,
        )


class TastingExtractorTest(ExtractorTestStrategy):
    @property
    def name(self) -> str:
        return "tasting_notes"

    def run(self, soup: BeautifulSoup, url: str) -> ExtractorTestResult:
        extractor = TastingExtractor()
        result = extractor.extract(soup, url)
        return _build_result(
            name=self.name,
            extractor_name=extractor.name,
            extracted_data=result.data,
            media=list(result.media),
            discovered_urls=set(result.discovered_urls),
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

            meta = result.data.get("meta", {})
            if meta:
                print(f"media_count: {meta.get('media_count', 0)}")
                if meta.get("media_samples"):
                    print(
                        f"media_samples: {json.dumps(meta['media_samples'], ensure_ascii=False, indent=2, default=str)}"
                    )
                print(f"discovered_urls_count: {meta.get('discovered_urls_count', 0)}")
                if meta.get("discovered_urls_samples"):
                    print(
                        f"discovered_urls_samples: {json.dumps(meta['discovered_urls_samples'], ensure_ascii=False, indent=2, default=str)}"
                    )


def main() -> None:
    url = "https://www.josephperrier.com/en/champagnes-et-cuvees/cuvee-ciergelot-2020"
    html, _ = Fetcher().get(url)
    soup = BeautifulSoup(html, "html.parser")

    tester = ExtractorTester(
        strategies=[
            ProductNameExtractorTest(),
            ProductDescriptionExtractorTest(),
            GrapeCompositionExtractorTest(),
            TemperatureExtractorTest(),
            RightTechKVExtractorTest(),
            DataSheetExtractorTest(),
            AwardsExtractorTest(),
            TastingExtractorTest(),
            InternalLinksExtractorTest(),
        ]
    )

    tester.print_all(soup, url)


if __name__ == "__main__":
    main()