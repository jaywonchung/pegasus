#!/usr/bin/env bash

set -e

cargo fmt --all
cargo check --all
cargo clippy --all
cargo test
