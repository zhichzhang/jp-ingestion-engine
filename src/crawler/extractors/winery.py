# src/crawler/extractors/winery.py

from __future__ import annotations

from bs4 import BeautifulSoup, Tag
from src.crawler.extractors.base import BaseExtractor, ExtractionResult
from src.crawler.selectors.winery import WINERY_DESCRIPTION_PARENT_SELECTOR, WINERY_HISTORY_TIMELINE_DESC, \
    WINERY_HISTORY_TIMELINE_TITLE, WINERY_FAMILY_SPIRIT_NAME_SELECTOR, WINERY_FAMILY_SPIRIT_DESC_PARENT_SELECTOR


def _text(node) -> str | None:
    if not node:
        return None
    s = " ".join(node.get_text(" ", strip=True).split())
    return s or None


class WineryDescriptionExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "winery_description"

    @property
    def selector(self) -> str:
        return WINERY_DESCRIPTION_PARENT_SELECTOR

    def can_run(self, soup: BeautifulSoup) -> bool:
        return bool(soup.select_one(WINERY_DESCRIPTION_PARENT_SELECTOR))

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        parent = soup.select_one(self.selector)
        if parent is None:
            return ExtractionResult(data={"description": None})

        description = self._extract_description(parent)
        return ExtractionResult(data={"description": description})

    def _extract_description(self, parent: Tag) -> str | None:
        parts: list[str] = []

        for child in parent.children:
            if isinstance(child, Tag):
                if child.name in {"b", "br"}:
                    continue
                text = child.get_text(" ", strip=True)
            else:
                text = str(child).strip()

            text = text.strip().strip('"').strip("“”").strip()
            if text:
                parts.append(" ".join(text.split()))

        if not parts:
            text = parent.get_text(" ", strip=True)
            text = " ".join(text.split())
            return text or None

        return " ".join(parts).strip() or None

class WineryHistoryTimelineExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "history_timeline"

    @property
    def selector(self) -> str:
        return WINERY_HISTORY_TIMELINE_TITLE

    def can_run(self, soup: BeautifulSoup) -> bool:
        return bool(
            soup.select_one(WINERY_HISTORY_TIMELINE_TITLE)
            and soup.select_one(WINERY_HISTORY_TIMELINE_DESC)
        )

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        timeline: list[dict[str, str | None]] = []

        title_nodes = soup.select(WINERY_HISTORY_TIMELINE_TITLE)

        for title_el in title_nodes:
            if not isinstance(title_el, Tag):
                continue

            raw_title = self._clean_text(title_el.get_text(" ", strip=True))
            if not raw_title:
                continue

            year, label = self._parse_title(raw_title)

            desc_text = self._extract_desc_for_title(title_el)

            timeline.append(
                {
                    "year": raw_title,
                    "event": desc_text or label,
                }
            )

        return ExtractionResult(data={"history_timeline": timeline})

    def _extract_desc_for_title(self, title_el: Tag) -> str | None:
        container = self._find_item_container(title_el)
        if container is None:
            return None

        desc_el = container.select_one(WINERY_HISTORY_TIMELINE_DESC)
        if desc_el is None:
            return None

        parts: list[str] = []

        p_nodes = desc_el.select("p")
        if p_nodes:
            for p in p_nodes:
                text = self._clean_text(p.get_text(" ", strip=True))
                if text:
                    parts.append(text)
            return " ".join(parts).strip() or None

        text = self._clean_text(desc_el.get_text(" ", strip=True))
        return text or None

    def _find_item_container(self, title_el: Tag) -> Tag | None:
        parent = title_el.parent
        while isinstance(parent, Tag):
            if parent.select_one(WINERY_HISTORY_TIMELINE_TITLE) and parent.select_one(WINERY_HISTORY_TIMELINE_DESC):
                return parent
            parent = parent.parent
        return None

    def _parse_title(self, text: str) -> tuple[str | None, str]:
        if "-" in text:
            parts = text.split("-", 1)
            return parts[0].strip(), parts[1].strip()
        return None, text

    def _clean_text(self, text: str) -> str:
        return " ".join(text.split()).strip().strip('"').strip("“”").strip()


class WineryFamilySpiritExtractor(BaseExtractor):
    @property
    def name(self) -> str:
        return "family_spirit"

    @property
    def selector(self) -> str:
        return WINERY_FAMILY_SPIRIT_NAME_SELECTOR

    def can_run(self, soup: BeautifulSoup) -> bool:
        return bool(
            soup.select_one(WINERY_FAMILY_SPIRIT_NAME_SELECTOR)
            and soup.select_one(WINERY_FAMILY_SPIRIT_DESC_PARENT_SELECTOR)
        )

    def extract(self, soup: BeautifulSoup, url: str) -> ExtractionResult:
        family_spirit: dict[str, str] = {}

        name_nodes = soup.select(WINERY_FAMILY_SPIRIT_NAME_SELECTOR)
        desc_parent_nodes = soup.select(WINERY_FAMILY_SPIRIT_DESC_PARENT_SELECTOR)

        for name_el, desc_parent in zip(name_nodes, desc_parent_nodes):
            if not isinstance(name_el, Tag) or not isinstance(desc_parent, Tag):
                continue

            name = self._normalize_name(name_el.get_text(" ", strip=True))
            if not name:
                continue

            description = self._extract_description(desc_parent)
            family_spirit[name] = description or ""

        return ExtractionResult(data={"family_spirit": family_spirit})

    def _normalize_name(self, text: str) -> str:
        text = self._clean_text(text)

        words = text.split()
        words = [w.capitalize() for w in words]

        return " ".join(words)

    def _extract_description(self, parent: Tag) -> str | None:
        parts: list[str] = []

        ps = parent.select("p")
        if ps:
            for p in ps:
                text = self._clean_text(p.get_text(" ", strip=True))
                if text:
                    parts.append(text)
        else:
            text = self._clean_text(parent.get_text(" ", strip=True))
            if text:
                parts.append(text)

        return "\n".join(parts).strip() or None

    def _clean_text(self, text: str) -> str:
        return " ".join(text.split()).strip().strip('"').strip("“”").strip()