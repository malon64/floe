#!/usr/bin/env python3
import argparse
import csv
import itertools
from pathlib import Path

DEFAULT_BASE = Path("bench/data/uber-raw-data-apr14.csv")
DEFAULT_SIZES = [100_000, 1_000_000, 5_000_000]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate benchmark CSV inputs.")
    parser.add_argument(
        "--base",
        type=Path,
        default=DEFAULT_BASE,
        help="Path to base Uber CSV (default: bench/data/uber-raw-data-apr14.csv)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("bench/generated"),
        help="Output directory for generated CSV files",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default=",".join(str(size) for size in DEFAULT_SIZES),
        help="Comma-separated row counts to generate",
    )
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=10_000,
        help="Every Nth row will have an empty pickup_datetime",
    )
    return parser.parse_args()


def row_cycle(base_path: Path):
    while True:
        with base_path.open("r", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield row


def generate_file(base_path: Path, out_path: Path, target_rows: int, invalid_every: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = row_cycle(base_path)
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["row_id", "pickup_datetime", "lat", "lon", "base"])
        for idx, row in enumerate(itertools.islice(rows, target_rows), start=1):
            pickup_datetime = row.get("Date/Time", "")
            if invalid_every > 0 and idx % invalid_every == 0:
                pickup_datetime = ""
            writer.writerow(
                [
                    idx,
                    pickup_datetime,
                    row.get("Lat", ""),
                    row.get("Lon", ""),
                    row.get("Base", ""),
                ]
            )


def main() -> None:
    args = parse_args()
    sizes = [int(value.strip()) for value in args.sizes.split(",") if value.strip()]
    if not args.base.exists():
        raise SystemExit(f"Base file not found: {args.base}")
    for size in sizes:
        label = f"{size // 1_000_000}m" if size >= 1_000_000 else f"{size // 1_000}k"
        out_path = args.out_dir / f"uber_{label}.csv"
        print(f"Generating {out_path} with {size} rows...")
        generate_file(args.base, out_path, size, args.invalid_every)


if __name__ == "__main__":
    main()
