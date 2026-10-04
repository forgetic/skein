#!/bin/sh
# What CI runs: formatting, lints as errors, then both test suites in full
# (testing-strategy.md, section 8): the focused one, then the fuzzy one.
set -eu

cd "$(dirname "$0")/.."

cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
# The TLS machine on its own: the workspace's build unifies rustls's std,
# which tests/tls turns on, and would hide a use of it.
cargo check -p skein-tls
cargo nextest run --workspace
cargo nextest run --workspace --profile fuzzy
