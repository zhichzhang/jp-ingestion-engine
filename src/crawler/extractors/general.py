# src/crawler/extractors/general.py

from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.crawler.selectors.general import (
    LANG_SWITCHER_ROOT,
    LANG_SWITCHER_EN,
    LANG_SWITCHER_FR,
    LANG_SWITCHER_ALL,
    LANG_SWITCHER_FALLBACK_EN, PAGE_LINKS_SELECTOR, PAGE_MEDIA_SELECTOR,
)

from src.crawler.extractors.base import BaseExtractor, ExtractionResult
from src.crawler.normalize import normalize_url, absolute, normalize_media_url
from src.pipeline.models import MediaRecord


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


class LanguageSwitchUrlExtractor(BaseExtractor):
    def __init__(self, target_lang: str = "en") -> None:
        self.target_lang = target_lang.lower().strip()

    @property
    def name(self) -> str:
        return f"language_switch_{self.target_lang}"

    @property
    def selector(self) -> str:
        return LANG_SWITCHER_ROOT

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        target_lang = self.target_lang

        if target_lang == "en":
            node = soup.select_one(LANG_SWITCHER_EN)
        elif target_lang == "fr":
            node = soup.select_one(LANG_SWITCHER_FR)
        else:
            node = soup.select_one(
                f'{LANG_SWITCHER_ROOT} li.weglot-language.weglot-{target_lang} a[href]'
            )

        if node:
            href = (node.get("href") or "").strip()
            if href:
                return ExtractionResult(
                    data={f"{target_lang}_url": absolute(url, href)}
                )

        for a in soup.select(LANG_SWITCHER_ALL):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if f"/{target_lang}/" in href:
                return ExtractionResult(
                    data={f"{target_lang}_url": absolute(url, href)}
                )

        if target_lang == "en":
            node = soup.select_one(LANG_SWITCHER_FALLBACK_EN)
            if node:
                href = (node.get("href") or "").strip()
                if href:
                    return ExtractionResult(
                        data={f"{target_lang}_url": absolute(url, href)}
                    )

        return ExtractionResult(data={f"{target_lang}_url": None})


_BG_URL_RE = re.compile(r'url\(["\']?(.*?)["\']?\)')


class MediaExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "media"

    @property
    def selector(self) -> str:
        return PAGE_MEDIA_SELECTOR

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        media: list[MediaRecord] = []
        seen_urls: set[str] = set()

        for node in soup.select(self.selector):
            if not isinstance(node, Tag):
                continue

            item = self._extract_media_item(node, url)
            if item is None:
                continue

            media_url = normalize_media_url(item["url"])
            if media_url in seen_urls:
                continue
            seen_urls.add(media_url)

            media.append(
                MediaRecord(
                    media_type=item["media_type"],
                    url=media_url,
                    source_page_url=url,
                )
            )

        return ExtractionResult(
            data={"media_count": len(media)},
            media=media,
        )

    def _extract_media_item(self, node: Tag, page_url: str) -> dict[str, str] | None:
        if node.name == "img":
            raw_url = self._pick_best_img_url(node)
            if raw_url:
                return {"media_type": "image", "url": urljoin(page_url, raw_url)}
            return None

        if node.name == "video":
            raw_url = node.get("src") or self._first_child_source_url(node)
            if raw_url:
                return {"media_type": "video", "url": urljoin(page_url, raw_url)}
            return None

        if node.name == "source":
            parent = node.parent if isinstance(node.parent, Tag) else None
            if parent is not None and parent.name == "video":
                raw_url = node.get("src") or node.get("srcset")
                if raw_url:
                    raw_url = self._pick_best_srcset_url(raw_url)
                    return {"media_type": "video", "url": urljoin(page_url, raw_url)}

            if parent is not None and parent.name == "picture":
                raw_url = node.get("srcset") or node.get("src")
                if raw_url:
                    raw_url = self._pick_best_srcset_url(raw_url)
                    return {"media_type": "image", "url": urljoin(page_url, raw_url)}

            return None

        if node.has_attr("data-dce-background-image-url"):
            raw_url = (node.get("data-dce-background-image-url") or "").strip()
            if raw_url:
                return {"media_type": "image", "url": urljoin(page_url, raw_url)}
            return None

        style = (node.get("style") or "").strip()
        if "background-image" in style:
            match = _BG_URL_RE.search(style)
            if match:
                raw_url = match.group(1).strip()
                if raw_url:
                    return {"media_type": "image", "url": urljoin(page_url, raw_url)}

        return None

    def _pick_best_img_url(self, img: Tag) -> str | None:
        srcset = (
            img.get("srcset")
            or img.get("data-srcset")
            or img.get("data-lazy-srcset")
            or ""
        ).strip()

        best_from_srcset = self._pick_best_srcset_url(srcset)
        if best_from_srcset:
            return best_from_srcset

        for attr in ("src", "data-src", "data-lazy-src", "data-original"):
            raw = (img.get(attr) or "").strip()
            if raw:
                return raw

        return None

    def _pick_best_srcset_url(self, srcset: str) -> str | None:
        if not srcset:
            return None

        candidates: list[tuple[int, str]] = []
        last_url: str | None = None

        for part in srcset.split(","):
            chunk = part.strip()
            if not chunk:
                continue

            pieces = chunk.split()
            if not pieces:
                continue

            url = pieces[0].strip()
            last_url = url

            width = 0
            if len(pieces) >= 2:
                raw_width = pieces[1].strip().lower()
                if raw_width.endswith("w"):
                    try:
                        width = int(raw_width[:-1])
                    except ValueError:
                        width = 0

            if url:
                candidates.append((width, url))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            return candidates[0][1]

        return last_url

    def _first_child_source_url(self, video: Tag) -> str | None:
        source = video.select_one("source[src], source[srcset]")
        if source is None:
            return None

        raw = (source.get("src") or source.get("srcset") or "").strip()
        if not raw:
            return None

        return self._pick_best_srcset_url(raw)