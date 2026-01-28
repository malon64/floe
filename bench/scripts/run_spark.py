#!/usr/bin/env python3
import argparse
import resource
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, Window


def rss_to_mb(value: float) -> float:
    if sys.platform == "darwin":
        return value / (1024 * 1024)
    return value / 1024


def label_for_size(size: int) -> str:
    if size >= 1_000_000:
        return f"{size // 1_000_000}m"
    return f"{size // 1_000}k"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark benchmark.")
    parser.add_argument(
        "--generated-dir",
        type=Path,
        default=Path("generated"),
        help="Directory with generated CSV files",
    )
    parser.add_argument(
        "--results-file",
        type=Path,
        default=Path("results/results.csv"),
        help="Results CSV path",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default="100000,1000000,5000000",
        help="Comma-separated row counts",
    )
    return parser.parse_args()


def run_for_size(
    spark: SparkSession, path: Path, accepted_path: Path, rejected_path: Path
) -> tuple[float, float, int, int]:
    start = time.perf_counter()

    df = spark.read.option("header", "true").csv(str(path))
    df = (
        df.withColumn("row_id", F.col("row_id").cast("long"))
        .withColumn(
            "pickup_datetime",
            F.to_timestamp("pickup_datetime", "M/d/yyyy H:mm:ss"),
        )
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lon", F.col("lon").cast("double"))
        .withColumn("base", F.col("base").cast("string"))
    )

    window = Window.partitionBy("row_id").orderBy("row_id")
    df = df.withColumn("dup_rank", F.row_number().over(window))

    valid = F.col("row_id").isNotNull()
    accepted_df = df.filter(valid & (F.col("dup_rank") == 1)).drop("dup_rank")
    rejected_df = df.filter(~(valid & (F.col("dup_rank") == 1))).drop("dup_rank")

    accepted = accepted_df.count()
    rejected = rejected_df.count()

    accepted_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)
    accepted_df.write.mode("overwrite").option("header", "true").csv(
        str(accepted_path)
    )
    rejected_df.write.mode("overwrite").option("header", "true").csv(
        str(rejected_path)
    )

    wall_time_s = time.perf_counter() - start
    usage = resource.getrusage(resource.RUSAGE_SELF)
    peak_rss_mb = rss_to_mb(usage.ru_maxrss)
    return wall_time_s, peak_rss_mb, accepted, rejected


def main() -> None:
    args = parse_args()
    sizes = [int(value.strip()) for value in args.sizes.split(",") if value.strip()]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("floe-bench")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    for size in sizes:
        label = label_for_size(size)
        path = args.generated_dir / f"uber_{label}.csv"
        if not path.exists():
            spark.stop()
            raise SystemExit(f"missing input: {path}")
        accepted_path = Path("out/accepted/spark") / f"uber_{label}"
        rejected_path = Path("out/rejected/spark") / f"uber_{label}"
        wall_time_s, peak_rss_mb, accepted, rejected = run_for_size(
            spark, path, accepted_path, rejected_path
        )
        cmd = [
            sys.executable,
            "scripts/write_result.py",
            "--results-file",
            str(args.results_file),
            "--tool",
            "spark",
            "--dataset",
            "uber",
            "--rows",
            str(size),
            "--wall-time",
            f"{wall_time_s:.4f}",
            "--peak-rss",
            f"{peak_rss_mb:.2f}",
            "--accepted",
            str(accepted),
            "--rejected",
            str(rejected),
            "--notes",
            "local[*]",
        ]
        import subprocess as sp

        sp.run(cmd, check=True)
        print(f"spark {label} done")

    spark.stop()


if __name__ == "__main__":
    main()
