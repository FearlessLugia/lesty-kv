#!/usr/bin/env python3
"""Publish benchmark CSV files and plots from a build directory."""

from __future__ import annotations

import argparse
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PLOT_CACHE = REPOSITORY_ROOT / "build" / ".plot-cache"
PLOT_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(PLOT_CACHE / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(PLOT_CACHE))

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class Benchmark:
    title: str
    source_csv: str
    published_csv: str
    plot_name: str
    throughput_column: str


BENCHMARKS = (
    Benchmark("Put", "experiment_Put.csv", "put_throughput.csv", "put_throughput.png", "Put Throughput"),
    Benchmark("Get", "experiment_Get.csv", "get_throughput.csv", "get_throughput.png", "Binary Search Throughput"),
    Benchmark("Scan", "experiment_Scan.csv", "scan_throughput.csv", "scan_throughput.png", "Scan Throughput"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and publish benchmark artifacts from CSV outputs."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=REPOSITORY_ROOT / "build",
        help="Directory containing CSV output from kv-experiment (default: <repo>/build).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPOSITORY_ROOT / "docs" / "benchmarks",
        help="Directory for published CSV files and plots (default: <repo>/docs/benchmarks).",
    )
    parser.add_argument(
        "--keep-input-csv",
        action="store_true",
        help="Copy CSV files instead of moving them out of the input directory.",
    )
    return parser.parse_args()


def write_plot(data: pd.DataFrame, benchmark: Benchmark, output_path: Path) -> None:
    figure, axis = plt.subplots(figsize=(8, 8))
    axis.plot(
        data["Data Size"],
        data[benchmark.throughput_column],
        marker="o",
        linestyle="-",
        linewidth=2.5,
        label=f"{benchmark.title} throughput",
    )

    sizes = data["Data Size"].tolist()
    axis.set_xscale("log", base=2)
    axis.set_xticks(sizes, labels=[str(size) for size in sizes])
    axis.set_title(f"{benchmark.title} Throughput vs Data Size")
    axis.set_xlabel("Data size (MiB, log scale)")
    axis.set_ylabel("Throughput (ops/s)")
    axis.grid(axis="y", alpha=0.2)
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)


def publish_csv(source: Path, destination: Path, keep_input: bool) -> None:
    if source.resolve() == destination.resolve():
        return

    if destination.exists():
        destination.unlink()

    if keep_input:
        shutil.copy2(source, destination)
    else:
        shutil.move(source, destination)


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not input_dir.is_dir():
        raise SystemExit(f"Benchmark input directory does not exist: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    for benchmark in BENCHMARKS:
        source_csv = input_dir / benchmark.source_csv
        if not source_csv.is_file():
            raise SystemExit(f"Missing benchmark CSV: {source_csv}")

        data = pd.read_csv(source_csv)
        write_plot(data, benchmark, output_dir / benchmark.plot_name)
        publish_csv(source_csv, output_dir / benchmark.published_csv, args.keep_input_csv)

    for legacy_plot in ("put_plot.png", "get_plot.png", "scan_plot.png"):
        (input_dir / legacy_plot).unlink(missing_ok=True)

    print(f"Published benchmark artifacts to {output_dir}")


if __name__ == "__main__":
    main()
