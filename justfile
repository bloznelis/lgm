# This menu
help:
  just --list

# Run the program
run:
  cargo run -- --config config/local.toml


# Build the binary
build:
  cargo build --release
