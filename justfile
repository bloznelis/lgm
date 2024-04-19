# This menu
help:
  just --list

# Check the program
check:
  cargo check

# Run the program
run:
  cargo run -- --config config/local.toml

# Build the binary
build:
  cargo build --release

# Release a new version
release:
  ./scripts/release.sh
