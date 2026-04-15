# src/cli/inspect.py

from __future__ import annotations

import logging
import sys

from src.db.supabase_client import get_client

logger = logging.getLogger("cli")


def list_products(limit: int = 100) -> None:
    logger.info("[CLI] list-products")

    client = get_client()
    try:
        resp = (
            client.table("products")
            .select(
                "name,product_url,source_page_url,description,"
                "dosage_g_per_l,aging,operating_temperature,crus_assembles,millennium,"
                "grape_chardonnay_percent,grape_pinot_noir_percent,grape_meunier_percent,"
                "awards_and_ratings,data_sheet_url"
            )
            .limit(limit)
            .execute()
        )

        rows = resp.data or []

        if not rows:
            logger.warning("[CLI] no products found")
            return

        logger.info("[CLI] fetched %d products", len(rows))

        for row in rows:
            print(
                f'{row.get("name")} | '
                f'{row.get("product_url")} | '
                f'{row.get("dosage_g_per_l")} | '
                f'{row.get("aging")} | '
                f'{row.get("operating_temperature")} | '
                f'{row.get("millennium")} | '
                f'{row.get("data_sheet_url")}'
            )

    except Exception as e:
        logger.error("[CLI] failed to list products: %s", e)


def show_product(name: str) -> None:
    logger.info("[CLI] show-product: %s", name)

    client = get_client()
    try:
        resp = (
            client.table("products")
            .select("*")
            .eq("name", name)
            .execute()
        )

        rows = resp.data or []

        if not rows:
            logger.warning("[CLI] product not found: %s", name)
            return

        logger.info("[CLI] found %d records", len(rows))

        for row in rows:
            print("=" * 60)
            for k, v in row.items():
                print(f"{k}: {v}")

    except Exception as e:
        logger.error("[CLI] failed to fetch product: %s", e)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(prog="inspect")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list-products", help="List products")
    list_parser.add_argument("--limit", type=int, default=100)

    show_parser = subparsers.add_parser("show-product", help="Show one product")
    show_parser.add_argument("name")

    args = parser.parse_args()

    if args.command == "list-products":
        list_products(limit=args.limit)
    elif args.command == "show-product":
        show_product(args.name)