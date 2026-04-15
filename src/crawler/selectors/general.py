# src/crawler/selectors/general.py

LISTING_CARD = 'div[data-elementor-type="loop-item"]'
LISTING_TITLE = 'p.elementor-heading-title a'
LISTING_SUMMARY = 'div.elementor-widget-text-editor p'
PAGE_LINKS_SELECTOR = "a[href]"
LANG_SWITCHER_ROOT = 'nav[aria-label="Menu"]'
LANG_SWITCHER_EN = 'nav[aria-label="Menu"] li.weglot-language.weglot-en a[href]'
LANG_SWITCHER_FR = 'nav[aria-label="Menu"] li.weglot-language.weglot-fr a[href]'
LANG_SWITCHER_ALL = 'nav[aria-label="Menu"] li.weglot-language a[href]'
LANG_SWITCHER_FALLBACK_EN = (
    'nav[aria-label="Menu"] a[title="English"][href], '
    'nav[aria-label="Menu"] a[href*="/en/"]'
)
PAGE_MEDIA_SELECTOR = (
    "img, "
    "video, "
    "video source, "
    "picture source, "
    "[data-dce-background-image-url], "
    "[style*='background-image']"
)
PAGE_MEDIA_IMAGE_SELECTOR = (
    "img, "
    "picture source, "
    "[data-dce-background-image-url], "
    "[style*='background-image']"
)
PAGE_MEDIA_VIDEO_SELECTOR = "video, video source"
PAGE_MEDIA_BACKGROUND_SELECTOR = "[data-dce-background-image-url], [style*='background-image']"