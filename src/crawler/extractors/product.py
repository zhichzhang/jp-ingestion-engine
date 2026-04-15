# src/crawler/selectors/product.py

from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup

from src.crawler.normalize import absolute, normalize_url
from src.crawler.extractors.base import BaseExtractor, ExtractionResult
from src.crawler.selectors.product import PRODUCT_DESCRIPTION_SELECTOR, PRODUCT_LEFT_TECH_KV_NODES_SELECTOR, \
    PRODUCT_RIGHT_TECH_KV_NODES_SELECTOR, PRODUCT_DATA_SHEET_URL_SELECTOR, PRODUCT_AWARDS_AND_RATING_SELECTOR, \
    PRODUCT_HEADING_SELECTOR, PRODUCT_TASTING_SELECTOR

from src.crawler.selectors.general import PAGE_LINKS_SELECTOR


def _text(node) -> str | None:
    if not node:
        return None
    s = " ".join(node.get_text(" ", strip=True).split())
    return s or None


def _clean_number(value: str | None) -> float | None:
    if not value:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)", value)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _clean_percent(value: str | None) -> float | None:
    return _clean_number(value)


def _clean_integer(value: str | None) -> int | None:
    if not value:
        return None
    m = re.search(r"(\d{4})", value)
    if not m:
        return None
    return int(m.group(1))


def _parse_year_from_text(text: str | None) -> int | None:
    if not text:
        return None
    lower = text.lower()
    if "non-vintage" in lower or "non vintage" in lower or lower.strip() == "nv":
        return None
    return _clean_integer(text)


def _extract_range(text: str | None) -> tuple[float | int | None, float | int | None]:
    if not text:
        return None, None

    nums = re.findall(r"\d+(?:[.,]\d+)?", text)
    if not nums:
        return None, None

    if len(nums) >= 2:
        a = float(nums[0].replace(",", "."))
        b = float(nums[1].replace(",", "."))
        return a, b

    v = float(nums[0].replace(",", "."))
    return v, v


def _normalize_tech_key(key: str) -> str:
    return " ".join(key.lower().split())


def _find_tech_value(kv: dict[str, Any], patterns: list[str]) -> str | None:
    for key, value in kv.items():
        if any(p in key for p in patterns):
            return value
    return None


class ProductNameExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "product_name"

    @property
    def selector(self) -> str:
        return PRODUCT_HEADING_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        root = soup.select_one('div[data-elementor-type="product"]')
        if not root:
            return ExtractionResult(data={"name": None})

        name = None
        subtitle = None

        for node in root.select(".elementor-heading-title"):
            text = _text(node)
            if not text:
                continue

            if node.name == "h1":
                name = text
            elif node.name == "p":
                if subtitle is None:
                    subtitle = text

        final_name = " ".join(filter(None, [name, subtitle]))

        return ExtractionResult(data={"name": final_name or None})


class ProductDescriptionExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "product_description"

    @property
    def selector(self) -> str:
        return PRODUCT_DESCRIPTION_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        nodes = soup.select(self.selector)
        texts: list[str] = []
        for n in nodes:
            t = _text(n)
            if t:
                texts.append(t)
        return ExtractionResult(data={"description": " ".join(texts) if texts else None})


class GrapeCompositionExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "grape_composition"

    @property
    def selector(self) -> str:
        return PRODUCT_LEFT_TECH_KV_NODES_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        nodes = soup.select(self.selector)

        kv: dict[str, str] = {}
        for i in range(0, min(len(nodes), 6), 2):
            if i + 1 >= len(nodes):
                break
            key = _text(nodes[i])
            value = _text(nodes[i + 1])
            if key and value:
                kv[_normalize_tech_key(key)] = value

        data: dict[str, Any] = {
            "grape_chardonnay_percent": None,
            "grape_pinot_noir_percent": None,
            "grape_meunier_percent": None,
        }

        for key, value in kv.items():
            pct = _clean_percent(value)
            if pct is None:
                continue

            if "chardonnay" in key:
                data["grape_chardonnay_percent"] = pct
            elif "pinot" in key and "noir" in key:
                data["grape_pinot_noir_percent"] = pct
            elif "meunier" in key or "pinot meunier" in key:
                data["grape_meunier_percent"] = pct

        return ExtractionResult(data=data)


class TemperatureExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "operating_temperature"

    @property
    def selector(self) -> str:
        return PRODUCT_LEFT_TECH_KV_NODES_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        nodes = soup.select(self.selector)

        for i in range(0, len(nodes) - 1, 2):
            key = _text(nodes[i])
            value = _text(nodes[i + 1])

            if not key or not value:
                continue

            normalized_key = _normalize_tech_key(key)
            if "operating temperature" in normalized_key or "temperature" in normalized_key:
                return ExtractionResult(data={"operating_temperature": value})

        return ExtractionResult(data={"operating_temperature": None})


class RightTechKVExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "right_tech_kv"

    @property
    def selector(self) -> str:
        return PRODUCT_RIGHT_TECH_KV_NODES_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        nodes = soup.select(self.selector)
        kv: dict[str, Any] = {}

        for i in range(0, len(nodes) - 1, 2):
            key = _text(nodes[i])
            value = _text(nodes[i + 1])
            if key and value:
                kv[_normalize_tech_key(key)] = value

        data: dict[str, Any] = {
            "dosage_g_per_l": None,
            "aging": None,
            "crus_assembles": None,
            "millennium": None,
        }

        dosage = _find_tech_value(kv, ["dosage"])
        if dosage:
            data["dosage_g_per_l"] = _clean_number(dosage)

        aging = _find_tech_value(kv, ["aging", "ageing", "maturation", "vieillissement"])
        if aging:
            data["aging"] = aging

        crus = _find_tech_value(kv, ["crus assembl", "crus assemb", "assembl", "blend"])
        if crus:
            data["crus_assembles"] = crus

        millennium = _find_tech_value(kv, ["millennium", "millésime", "vintage"])
        if millennium:
            data["millennium"] = millennium

        return ExtractionResult(data=data)


class DataSheetExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "data_sheet"

    @property
    def selector(self) -> str:
        return PRODUCT_DATA_SHEET_URL_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        node = soup.select_one(self.selector)
        if not node:
            return ExtractionResult(data={"data_sheet_url": None})

        href = (node.get("href") or "").strip()
        if not href:
            return ExtractionResult(data={"data_sheet_url": None})

        return ExtractionResult(
            data={"data_sheet_url": normalize_url(absolute(url, href))}
        )


class AwardsExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "awards"

    @property
    def selector(self) -> str:
        return PRODUCT_AWARDS_AND_RATING_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        nodes = soup.select(self.selector)
        awards: list[dict[str, str]] = []

        for i in range(0, len(nodes) - 1, 2):
            source = _text(nodes[i])
            value = _text(nodes[i + 1])
            if source:
                awards.append(
                    {
                        "title": source,
                        "value": value or "",
                    }
                )

        return ExtractionResult(data={"awards_and_ratings": awards})


class TastingExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "tasting"

    @property
    def selector(self) -> str:
        return PRODUCT_TASTING_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        nodes = soup.select(self.selector)

        items: list[dict[str, str]] = []
        for i in range(0, len(nodes) - 1, 2):
            title = _text(nodes[i])
            content = _text(nodes[i + 1])

            if title:
                items.append(
                    {
                        "title": title,
                        "content": content or "",
                    }
                )

        return ExtractionResult(data={"tasting_notes": items})


class InternalLinksExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "internal_links"

    @property
    def selector(self) -> str:
        return PAGE_LINKS_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        links: set[str] = set()

        for a in soup.select(self.selector):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("/") or "josephperrier.com" in href:
                links.add(normalize_url(absolute(url, href)))

        return ExtractionResult(discovered_urls=links)