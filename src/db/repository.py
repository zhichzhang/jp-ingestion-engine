# src/db/repository.py

from __future__ import annotations

import logging
from typing import Any

from supabase import Client

from src.crawler.normalize import normalize_url, normalize_media_url
from src.pipeline.models import MediaRecord, ProductRecord, WineryRecord

logger = logging.getLogger("db")


def _norm_key(value: str | None) -> str:
    return (value or "").strip().lower()


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _drop_none(row: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v
        for k, v in row.items()
        if v is not None and k not in {"id", "created_at", "updated_at"}
    }


def _merge_scalar(old: Any, new: Any) -> Any:
    if _is_empty(new):
        return old
    return new


def _merge_dict(old: Any, new: Any) -> dict[str, Any]:
    old_dict = old if isinstance(old, dict) else {}
    new_dict = new if isinstance(new, dict) else {}

    merged: dict[str, Any] = dict(old_dict)
    for key, value in new_dict.items():
        if key in merged:
            merged[key] = _merge_value(merged[key], value)
        elif not _is_empty(value):
            merged[key] = value
    return merged


def _merge_list_of_dicts(
    old: Any,
    new: Any,
    key_fields: tuple[str, str] = ("year", "event"),
) -> list[dict[str, Any]]:
    old_list = old if isinstance(old, list) else []
    new_list = new if isinstance(new, list) else []

    seen: set[tuple[str, str]] = set()
    merged: list[dict[str, Any]] = []

    for item in old_list + new_list:
        if not isinstance(item, dict):
            continue

        a = str(item.get(key_fields[0]) or "").strip()
        b = str(item.get(key_fields[1]) or "").strip()
        key = (a, b)
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)

    return merged


def _merge_awards(old: Any, new: Any) -> list[dict[str, Any]]:
    return _merge_list_of_dicts(old, new, key_fields=("title", "value"))


def _merge_value(old: Any, new: Any) -> Any:
    if isinstance(old, dict) or isinstance(new, dict):
        return _merge_dict(old, new)

    if isinstance(old, list) or isinstance(new, list):
        return _merge_list_of_dicts(old, new)

    return _merge_scalar(old, new)


class Repository:
    def __init__(self, client: Client) -> None:
        self.client = client

    @staticmethod
    def _score_product(p: ProductRecord) -> int:
        fields = [
            p.description,
            p.dosage_g_per_l,
            p.aging,
            p.operating_temperature,
            p.crus_assembles,
            p.millennium,
            p.grape_chardonnay_percent,
            p.grape_pinot_noir_percent,
            p.grape_meunier_percent,
            p.awards_and_ratings,
            p.data_sheet_url,
        ]
        return sum(v not in (None, "", [], {}) for v in fields)

    @staticmethod
    def _merge_winery_records(items: list[WineryRecord]) -> WineryRecord:
        base = items[0]

        description = base.description
        family_spirit = dict(base.family_spirit or {})
        history_timeline = list(base.history_timeline or [])
        website_url = base.website_url
        source_page_url = base.source_page_url

        for w in items[1:]:
            description = _merge_scalar(description, w.description)
            family_spirit = _merge_dict(family_spirit, w.family_spirit)
            history_timeline = _merge_list_of_dicts(history_timeline, w.history_timeline)

            website_url = _merge_scalar(website_url, w.website_url)
            source_page_url = _merge_scalar(source_page_url, w.source_page_url)

        return WineryRecord(
            name=base.name,
            website_url=website_url,
            source_page_url=source_page_url,
            description=description,
            family_spirit=family_spirit,
            history_timeline=history_timeline,
        )

    @staticmethod
    def _merge_winery_with_existing(
        existing: dict[str, Any] | None,
        current: WineryRecord,
    ) -> dict[str, Any]:
        row = _drop_none(current.to_db_dict())

        if not existing:
            return row

        row["description"] = _merge_scalar(existing.get("description"), row.get("description"))
        row["family_spirit"] = _merge_dict(existing.get("family_spirit"), row.get("family_spirit"))
        row["history_timeline"] = _merge_list_of_dicts(
            existing.get("history_timeline"),
            row.get("history_timeline"),
        )

        row["website_url"] = _merge_scalar(existing.get("website_url"), row.get("website_url"))
        row["source_page_url"] = _merge_scalar(existing.get("source_page_url"), row.get("source_page_url"))

        return row

    @staticmethod
    def _merge_product_records(items: list[ProductRecord]) -> ProductRecord:
        base = items[0]

        description = base.description
        dosage_g_per_l = base.dosage_g_per_l
        aging = base.aging
        operating_temperature = base.operating_temperature
        crus_assembles = base.crus_assembles
        millennium = base.millennium
        grape_chardonnay_percent = base.grape_chardonnay_percent
        grape_pinot_noir_percent = base.grape_pinot_noir_percent
        grape_meunier_percent = base.grape_meunier_percent
        awards_and_ratings = list(base.awards_and_ratings or [])
        data_sheet_url = base.data_sheet_url
        winery_id = base.winery_id
        source_page_url = base.source_page_url
        name = base.name
        product_url = base.product_url

        for p in items[1:]:
            description = _merge_scalar(description, p.description)
            dosage_g_per_l = _merge_scalar(dosage_g_per_l, p.dosage_g_per_l)
            aging = _merge_scalar(aging, p.aging)
            operating_temperature = _merge_scalar(operating_temperature, p.operating_temperature)
            crus_assembles = _merge_scalar(crus_assembles, p.crus_assembles)
            millennium = _merge_scalar(millennium, p.millennium)
            grape_chardonnay_percent = _merge_scalar(grape_chardonnay_percent, p.grape_chardonnay_percent)
            grape_pinot_noir_percent = _merge_scalar(grape_pinot_noir_percent, p.grape_pinot_noir_percent)
            grape_meunier_percent = _merge_scalar(grape_meunier_percent, p.grape_meunier_percent)
            awards_and_ratings = _merge_awards(awards_and_ratings, p.awards_and_ratings)
            data_sheet_url = _merge_scalar(data_sheet_url, p.data_sheet_url)
            winery_id = _merge_scalar(winery_id, p.winery_id)
            source_page_url = _merge_scalar(source_page_url, p.source_page_url)
            name = _merge_scalar(name, p.name)
            product_url = _merge_scalar(product_url, p.product_url)

        return ProductRecord(
            winery_id=winery_id,
            name=name,
            product_url=product_url,
            source_page_url=source_page_url,
            description=description,
            dosage_g_per_l=dosage_g_per_l,
            aging=aging,
            operating_temperature=operating_temperature,
            crus_assembles=crus_assembles,
            millennium=millennium,
            grape_chardonnay_percent=grape_chardonnay_percent,
            grape_pinot_noir_percent=grape_pinot_noir_percent,
            grape_meunier_percent=grape_meunier_percent,
            awards_and_ratings=awards_and_ratings,
            data_sheet_url=data_sheet_url,
        )

    @staticmethod
    def _merge_product_with_existing(
        existing: dict[str, Any] | None,
        current: ProductRecord,
        resolved_winery_id: str,
    ) -> dict[str, Any]:
        row = _drop_none(current.to_db_dict())
        row["winery_id"] = resolved_winery_id

        if not existing:
            row["product_url"] = normalize_url(current.product_url)
            return row

        row["description"] = _merge_scalar(existing.get("description"), row.get("description"))
        row["dosage_g_per_l"] = _merge_scalar(existing.get("dosage_g_per_l"), row.get("dosage_g_per_l"))
        row["aging"] = _merge_scalar(existing.get("aging"), row.get("aging"))
        row["operating_temperature"] = _merge_scalar(
            existing.get("operating_temperature"),
            row.get("operating_temperature"),
        )
        row["crus_assembles"] = _merge_scalar(existing.get("crus_assembles"), row.get("crus_assembles"))
        row["millennium"] = _merge_scalar(existing.get("millennium"), row.get("millennium"))
        row["grape_chardonnay_percent"] = _merge_scalar(
            existing.get("grape_chardonnay_percent"),
            row.get("grape_chardonnay_percent"),
        )
        row["grape_pinot_noir_percent"] = _merge_scalar(
            existing.get("grape_pinot_noir_percent"),
            row.get("grape_pinot_noir_percent"),
        )
        row["grape_meunier_percent"] = _merge_scalar(
            existing.get("grape_meunier_percent"),
            row.get("grape_meunier_percent"),
        )
        row["awards_and_ratings"] = _merge_awards(existing.get("awards_and_ratings"), row.get("awards_and_ratings"))
        row["data_sheet_url"] = _merge_scalar(existing.get("data_sheet_url"), row.get("data_sheet_url"))

        row["source_page_url"] = _merge_scalar(existing.get("source_page_url"), row.get("source_page_url"))
        row["product_url"] = normalize_url(current.product_url)

        return row

    def upsert_wineries(self, wineries: list[WineryRecord]) -> dict[str, str]:
        if not wineries:
            logger.debug("[WINERIES] skip empty batch")
            return {}

        grouped: dict[str, list[WineryRecord]] = {}
        for w in wineries:
            if not w.name or not w.website_url or not w.source_page_url:
                continue
            grouped.setdefault(_norm_key(w.name), []).append(w)

        if not grouped:
            logger.warning("[WINERIES] no valid rows to upsert")
            return {}

        merged_batch = [self._merge_winery_records(items) for items in grouped.values()]
        names = [w.name.strip() for w in merged_batch]

        existing_resp = (
            self.client.table("wineries")
            .select("id,name,description,family_spirit,history_timeline,website_url,source_page_url")
            .in_("name", names)
            .execute()
        )
        existing_rows = existing_resp.data or []
        existing_map = {_norm_key(row["name"]): row for row in existing_rows}

        rows: list[dict[str, Any]] = []
        for w in merged_batch:
            current = existing_map.get(_norm_key(w.name))

            logger.info(
                "[WINERY MERGE INPUT] name=%s desc=%s family=%d history=%d",
                w.name,
                bool(w.description),
                len(w.family_spirit or {}),
                len(w.history_timeline or []),
            )

            logger.info(
                "[WINERY EXISTING] name=%s desc=%s family=%d history=%d",
                w.name,
                bool(current.get("description") if current else None),
                len(current.get("family_spirit") or {}) if current else 0,
                len(current.get("history_timeline") or []) if current else 0,
            )

            merged = self._merge_winery_with_existing(current, w)

            logger.info(
                "[WINERY MERGED RESULT] name=%s desc=%s family=%d history=%d",
                w.name,
                bool(merged.get("description")),
                len(merged.get("family_spirit") or {}),
                len(merged.get("history_timeline") or []),
            )

            rows.append(merged)

        logger.info("[WINERIES] upserting %d rows", len(rows))
        self.client.table("wineries").upsert(rows, on_conflict="name").execute()

        resp = self.client.table("wineries").select("id,name").in_("name", names).execute()
        data = resp.data or []
        return {_norm_key(row["name"]): row["id"] for row in data}

    def upsert_products(
        self,
        products: list[ProductRecord],
        winery_id: str | None = None,
    ) -> dict[str, str]:
        if not products:
            logger.debug("[PRODUCTS] skip empty batch")
            return {}

        grouped: dict[str, list[ProductRecord]] = {}
        for p in products:
            if not p.name or not p.product_url or not p.source_page_url:
                continue
            grouped.setdefault(normalize_url(p.product_url), []).append(p)

        if not grouped:
            logger.warning("[PRODUCTS] no valid rows to upsert")
            return {}

        merged_batch = [self._merge_product_records(items) for items in grouped.values()]

        urls = [normalize_url(p.product_url) for p in merged_batch]
        existing_resp = (
            self.client.table("products")
            .select(
                "id,winery_id,name,product_url,source_page_url,description,"
                "dosage_g_per_l,aging,operating_temperature,crus_assembles,millennium,"
                "grape_chardonnay_percent,grape_pinot_noir_percent,grape_meunier_percent,"
                "awards_and_ratings,data_sheet_url"
            )
            .in_("product_url", urls)
            .execute()
        )
        existing_rows = existing_resp.data or []
        existing_map = {normalize_url(row["product_url"]): row for row in existing_rows}

        rows: list[dict[str, Any]] = []
        for p in merged_batch:
            resolved_winery_id = p.winery_id or winery_id
            if not resolved_winery_id:
                logger.warning("[PRODUCTS] skip %r because winery_id is missing", p.name)
                continue

            current = existing_map.get(normalize_url(p.product_url))
            rows.append(self._merge_product_with_existing(current, p, resolved_winery_id))

        if not rows:
            logger.warning("[PRODUCTS] no rows to upsert after resolution")
            return {}

        logger.info("[PRODUCTS] upserting %d rows", len(rows))
        self.client.table("products").upsert(rows, on_conflict="product_url").execute()

        resp = (
            self.client.table("products")
            .select("id,product_url")
            .in_("product_url", [r["product_url"] for r in rows])
            .execute()
        )
        data = resp.data or []
        product_map: dict[str, str] = {}
        for row in data:
            product_map[normalize_url(row["product_url"])] = row["id"]

        logger.info("[PRODUCTS] resolved %d ids", len(product_map))
        return product_map

    def upsert_media(self, media: list[MediaRecord]) -> None:
        if not media:
            logger.debug("[MEDIA] skip empty batch")
            return

        deduped: dict[str, MediaRecord] = {}
        for m in media:
            if not m.media_type or not m.url or not m.source_page_url:
                continue

            key = normalize_media_url(m.url)
            if key in deduped:
                continue
            deduped[key] = m

        if not deduped:
            logger.warning("[MEDIA] no valid rows to upsert")
            return

        rows = [_drop_none(m.to_db_dict()) for m in deduped.values()]
        for row in rows:
            row["url"] = normalize_media_url(row["url"])

        logger.info("[MEDIA] upserting %d rows", len(rows))
        self.client.table("media").upsert(rows, on_conflict="url").execute()