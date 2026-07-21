#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
build_dir="$repository_root/build"

cmake -S "$repository_root" -B "$build_dir" -DCMAKE_BUILD_TYPE=Release
cmake --build "$build_dir"

(
    cd "$build_dir"
    ./kv-experiment
)

python3 "$repository_root/experiments/plot_generator.py"
