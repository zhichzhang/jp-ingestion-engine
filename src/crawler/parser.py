# src/crawler/parser.py
from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from src.config import settings
from src.crawler import selectors as S
from src.crawler.extractors.base import BaseExtractor
from src.crawler.extractors.general import (
    InternalLinksExtractor,
    LanguageSwitchUrlExtractor,
    MediaExtractor,
)
from src.crawler.extractors.product import (
    AwardsExtractor,
    DataSheetExtractor,
    GrapeCompositionExtractor,
    ProductDescriptionExtractor,
    ProductNameExtractor,
    RightTechKVExtractor,
    TemperatureExtractor,
)
from src.crawler.extractors.winery import (
    WineryDescriptionExtractor,
    WineryFamilySpiritExtractor,
    WineryHistoryTimelineExtractor,
)
from src.crawler.normalize import absolute, is_internal, normalize_url
from src.crawler.selectors.general import LISTING_CARD, LISTING_TITLE, LISTING_SUMMARY
from src.pipeline.models import MediaRecord, ParsedPage, ProductRecord, WineryRecord

logger = logging.getLogger("parser")

PRODUCT_PATH_RE = re.compile(r"^/(?:en/)?champagnes(?:-et)?-cuvees/[^/?#]+/?$", re.I)
CATALOG_PATH_RE = re.compile(r"^/(?:en/)?(?:champagnes(?:-cuvees)?|champagnes)/?$", re.I)
WINERY_PATH_RE = re.compile(
    r"^/(?:en/)?(?:maison(?:/|$)(?:histoire|famille|vignoble|savoirs-faire|cave)?|engagements(?:/|$))",
    re.I,
)

DETAIL_EXTRACTORS: list[BaseExtractor] = [
    ProductNameExtractor(),
    ProductDescriptionExtractor(),
    GrapeCompositionExtractor(),
    TemperatureExtractor(),
    RightTechKVExtractor(),
    DataSheetExtractor(),
    AwardsExtractor(),
    InternalLinksExtractor(),
    MediaExtractor(),
]

WINERY_EXTRACTORS: list[BaseExtractor] = [
    WineryDescriptionExtractor(),
    WineryHistoryTimelineExtractor(),
    WineryFamilySpiritExtractor(),
    InternalLinksExtractor(),
    MediaExtractor(),
]


def text(node) -> str | None:
    if not node:
        return None
    s = " ".join(node.get_text(" ", strip=True).split())
    return s or None


def slug_from_url(url: str) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.rstrip("/")
    if not path:
        return None
    return path.split("/")[-1] or None


def _page_path(url: str) -> str:
    return urlparse(url).path.rstrip("/")


def _is_product_page(url: str) -> bool:
    return bool(PRODUCT_PATH_RE.search(_page_path(url)))


def _is_catalog_page(url: str) -> bool:
    return bool(CATALOG_PATH_RE.search(_page_path(url)))


def _is_winery_page(url: str) -> bool:
    return bool(WINERY_PATH_RE.search(_page_path(url)))


def _run_extractors(
    soup: BeautifulSoup,
    url: str,
    extractors: list[BaseExtractor],
) -> tuple[dict[str, Any], list[MediaRecord], set[str]]:
    data: dict[str, Any] = {}
    media: list[MediaRecord] = []
    discovered: set[str] = set()

    for extractor in extractors:
        try:
            if not extractor.can_run(soup):
                continue
            result = extractor.extract(soup, url)
        except Exception:
            logger.exception("[EXTRACTOR][%s] failed url=%s", extractor.name, url)
            continue

        if result.data:
            for key, value in result.data.items():
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                data[key] = value

        if result.media:
            media.extend(result.media)

        if result.discovered_urls:
            discovered.update(result.discovered_urls)

    return data, media, discovered


def _extract_discovered_urls_from_links(soup: BeautifulSoup, base_url: str) -> set[str]:
    urls: set[str] = set()
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue

        abs_url = absolute(base_url, href)
        if is_internal(abs_url):
            urls.add(normalize_url(abs_url))
    return urls


def extract_language_switch_url(
    html: str,
    base_url: str,
    target_lang: str = "en",
) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    result = LanguageSwitchUrlExtractor(target_lang=target_lang).extract(soup, base_url)
    key = f"{target_lang.lower().strip()}_url"
    value = result.data.get(key)
    return value if isinstance(value, str) and value.strip() else None


def is_french_page(html: str, final_url: str | None = None) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    html_node = soup.find("html")
    if html_node:
        lang = (html_node.get("lang") or "").strip().lower()
        if lang.startswith("fr"):
            return True

    if final_url:
        path = _page_path(final_url)
        if path == "/fr" or path.startswith("/fr/"):
            return True

    return False


def is_english_page(html: str, final_url: str | None = None) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    html_node = soup.find("html")
    if html_node:
        lang = (html_node.get("lang") or "").strip().lower()
        if lang.startswith("en"):
            return True

    if final_url:
        path = _page_path(final_url)
        if path == "/en" or path.startswith("/en/"):
            return True

    return False


def parse_any_page(html: str, url: str) -> ParsedPage:
    logger.info("parse_any_page url=%r", url)

    path = _page_path(url).lower()

    if _is_product_page(url):
        return parse_product_detail_page(html, url)

    if _is_catalog_page(url):
        return parse_product_catalog_page(html, url)

    if "/maison/histoire" in path:
        return parse_winery_history_page(html, url)

    if "/maison/famille" in path:
        return parse_winery_family_spirit_page(html, url)

    if _is_winery_page(url) or path in {"/", "/en", "/fr"}:
        return parse_winery_home_page(html, url)

    return ParsedPage(None, [], [], set())


def parse_product_catalog_page(html: str, url: str) -> ParsedPage:
    logger.info("[CATALOG] parsing %s", url)
    soup = BeautifulSoup(html, "html.parser")

    products: list[ProductRecord] = []
    media: list[MediaRecord] = []
    discovered = _extract_discovered_urls_from_links(soup, url)

    for card in soup.select(LISTING_CARD):
        links: list[str] = []
        for a in card.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            abs_url = absolute(url, href)
            if is_internal(abs_url):
                links.append(normalize_url(abs_url))

        product_url = next((u for u in links if _is_product_page(u)), None)
        if not product_url:
            continue

        title = text(card.select_one(LISTING_TITLE))
        if not title:
            continue

        description = text(card.select_one(LISTING_SUMMARY))

        products.append(
            ProductRecord(
                winery_id=None,
                name=title,
                product_url=product_url,
                source_page_url=url,
                description=description,
                dosage_g_per_l=None,
                aging=None,
                operating_temperature=None,
                crus_assembles=None,
                millennium=None,
                grape_chardonnay_percent=None,
                grape_pinot_noir_percent=None,
                grape_meunier_percent=None,
                awards_and_ratings=[],
                data_sheet_url=None,
            )
        )

        media.extend(MediaExtractor().extract(card, url).media)

    logger.info("[CATALOG] done %s | products=%d media=%d", url, len(products), len(media))
    return ParsedPage(None, products, media, discovered)


def parse_product_detail_page(html: str, url: str) -> ParsedPage:
    logger.info("[DETAIL] parsing %s", url)
    soup = BeautifulSoup(html, "html.parser")

    data, media, discovered = _run_extractors(soup, url, DETAIL_EXTRACTORS)

    product = ProductRecord(
        winery_id=None,
        name=data["name"],
        product_url=normalize_url(url),
        source_page_url=url,
        description=data.get("description"),
        dosage_g_per_l=data.get("dosage_g_per_l"),
        aging=data.get("aging"),
        operating_temperature=data.get("operating_temperature"),
        crus_assembles=data.get("crus_assembles"),
        millennium=data.get("millennium"),
        grape_chardonnay_percent=data.get("grape_chardonnay_percent"),
        grape_pinot_noir_percent=data.get("grape_pinot_noir_percent"),
        grape_meunier_percent=data.get("grape_meunier_percent"),
        awards_and_ratings=data.get("awards_and_ratings", []),
        data_sheet_url=data.get("data_sheet_url"),
    )

    logger.info(
        "[DETAIL] done %s | name=%s | media=%d",
        url,
        product.name,
        len(media),
    )
    return ParsedPage(None, [product], media, discovered)


def parse_winery_home_page(html: str, url: str) -> ParsedPage:
    return parse_winery_page(html, url, page_type="winery")


def parse_winery_history_page(html: str, url: str) -> ParsedPage:
    return parse_winery_page(html, url, page_type="history")


def parse_winery_family_spirit_page(html: str, url: str) -> ParsedPage:
    return parse_winery_page(html, url, page_type="family_spirit")


def parse_winery_page(html: str, url: str, page_type: str | None = None) -> ParsedPage:
    logger.info("[WINERY] parsing %s", url)
    soup = BeautifulSoup(html, "html.parser")

    if page_type is None:
        path = _page_path(url).lower()
        if "/maison/histoire" in path:
            page_type = "history"
        elif "/maison/famille" in path:
            page_type = "family_spirit"
        else:
            page_type = "winery"

    data, media, discovered = _run_extractors(soup, url, WINERY_EXTRACTORS)

    winery = WineryRecord(
        name="Joseph Perrier",
        website_url=settings.base_url,
        source_page_url=url,
        description=data.get("description") if page_type == "winery" else None,
        family_spirit=data.get("family_spirit") if page_type == "family_spirit" else {},
        history_timeline=data.get("history_timeline") if page_type == "history" else [],
    )

    logger.info(
        "[WINERY] done %s | page_type=%s | media=%d",
        url,
        page_type,
        len(media),
    )
    return ParsedPage(winery, [], media, discovered)