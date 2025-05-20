#!/usr/bin/env python3
import argparse
from datetime import datetime
import sys
from pathlib import Path

from crawler.crawl import crawl, get_chains


def parse_date(date_str):
    """Parse a date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format")


def main():
    parser = argparse.ArgumentParser(
        description="Crawl retail chains for product pricing data",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "output_path",
        nargs="?",
        type=Path,
        default=None,
        help="Output directory path where data will be stored.\n(Required unless -l/--list is used)",
    )
    parser.add_argument(
        "-d",
        "--date",
        type=parse_date,
        help="Date for which to crawl (format: YYYY-MM-DD, defaults to today)",
    )
    parser.add_argument(
        "-c",
        "--chain",
        help="Comma-separated list of retail chains to crawl (defaults to all)",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List supported retail chains and exit (output_path is not required)",
    )

    args = parser.parse_args()

    if args.list:
        print("Supported retail chains:")
        for chain_name in get_chains():
            print(f"  - {chain_name}")
        return 0

    if args.output_path is None:
        parser.error("output_path is required; use -h/--help for more info")

    if args.output_path.is_file():
        parser.error(f"Output path '{args.output_path}' is a file.")

    if not args.output_path.exists():
        args.output_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {args.output_path}")

    chains_to_crawl = None
    if args.chain:
        chains_to_crawl = [chain.strip() for chain in args.chain.split(",")]
        available_chains = get_chains()
        for chain_name in chains_to_crawl:
            if chain_name not in available_chains:
                parser.error(
                    f"Unknown chain '{chain_name}'. Available chains: {', '.join(available_chains)}"
                )

    # Run the crawler
    try:
        # Ensure date is None if not provided, so crawl() uses its default
        crawl_date = args.date  # parse_date already handles empty string to None

        chains_txt = (
            ", ".join(chains_to_crawl) if chains_to_crawl else "all retail chains"
        )
        date_txt = args.date.strftime("%Y-%m-%d") if args.date else "today"
        print(f"Fetching price data from {chains_txt} for {date_txt} ...", flush=True)

        zip_path = crawl(args.output_path, crawl_date, chains_to_crawl)
        print(f"Archive created: {zip_path}")
        return 0
    except Exception as e:
        # Consider logging the full exception for debugging if a logger is set up
        print(f"Error during crawling: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
