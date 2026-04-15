from __future__ import annotations

import argparse

from src.config import settings
from src.pipeline.orchestrator import Orchestrator
from src.utils.logger import setup_logger
from src.cli.inspect import list_products, show_product


def crawl() -> None:
    setup_logger()
    orch = Orchestrator()
    result = orch.crawl([settings.base_url])
    print(result)


def positive_int(value: str) -> int:
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("limit must be a positive integer")
    return ivalue


def main() -> None:
    parser = argparse.ArgumentParser(prog="joseph-perrier")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # crawl
    subparsers.add_parser("crawl", help="Run the crawler")

    # list-products
    list_parser = subparsers.add_parser("list-products", help="List products")
    list_parser.add_argument(
        "--limit",
        type=positive_int,
        default=10,
        help="Maximum number of products to display"
    )

    # show-product
    show_parser = subparsers.add_parser("show-product", help="Show one product")
    show_parser.add_argument("name")

    args = parser.parse_args()

    if args.command == "crawl":
        crawl()
    elif args.command == "list-products":
        list_products(limit=args.limit)
    elif args.command == "show-product":
        show_product(args.name)


if __name__ == "__main__":
    main()