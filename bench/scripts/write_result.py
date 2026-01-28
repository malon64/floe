#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append a benchmark result row.")
    parser.add_argument("--results-file", type=Path, required=True)
    parser.add_argument("--tool", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--rows", required=True)
    parser.add_argument("--wall-time", required=True)
    parser.add_argument("--peak-rss", default="")
    parser.add_argument("--accepted", required=True)
    parser.add_argument("--rejected", required=True)
    parser.add_argument("--notes", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.results_file.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "tool",
        "dataset",
        "rows",
        "wall_time_s",
        "peak_rss_mb",
        "accepted_rows",
        "rejected_rows",
        "notes",
    ]
    write_header = not args.results_file.exists() or args.results_file.stat().st_size == 0

    with args.results_file.open("a", newline="") as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(header)
        writer.writerow(
            [
                args.tool,
                args.dataset,
                args.rows,
                args.wall_time,
                args.peak_rss,
                args.accepted,
                args.rejected,
                args.notes,
            ]
        )


if __name__ == "__main__":
    main()
