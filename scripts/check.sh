#!/bin/sh
# What CI runs: formatting, lints as errors, then the tests.
set -eu

cd "$(dirname "$0")/.."

cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
