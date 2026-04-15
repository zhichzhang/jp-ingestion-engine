# src/crawler/normalize.py

from urllib.parse import urljoin, urlparse, urlunparse


def normalize_url(url: str) -> str:
    if not url:
        return ""

    parsed = urlparse(url)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/"),
            "",
            "",
            "",
        )
    )

def normalize_media_url(url: str) -> str:
    if not url:
        return ""

    parsed = urlparse(url)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/"),
            "",
            parsed.query,
            "",
        )
    )

def absolute(base: str, href: str) -> str:
    if not href:
        return ""

    return urljoin(base, href)


def is_internal(url: str) -> bool:
    netloc = urlparse(url).netloc
    return netloc in ("", "www.josephperrier.com", "josephperrier.com")

def canonical_page_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return urlunparse((
        "https",
        "www.josephperrier.com",
        parsed.path.rstrip("/"),
        "",
        "",
        "",
    ))