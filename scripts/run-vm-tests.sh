#!/usr/bin/env bash
# Cross-compile test binaries on macOS (via Docker + cross) and run them in the Multipass VM.
set -euo pipefail

VM=bcr-dev
BTRFS_MOUNT=/mnt/bcr-test
S3_ENDPOINT=http://localhost:9000
S3_BUCKET=bcr-test
S3_USER=minioadmin
S3_PASS=minioadmin
TARGET=aarch64-unknown-linux-gnu

if ! command -v cross &>/dev/null; then
    echo "Installing cross..."
    cargo install cross
fi

rustup target add "$TARGET"

build_and_run() {
    local test_name=$1
    shift
    local env_args=("$@")

    echo "==> Building $test_name..."
    cross test --test "$test_name" --no-run --target "$TARGET"

    local bin
    bin=$(ls -t target/"$TARGET"/debug/deps/"${test_name}"-* 2>/dev/null | grep -v '\.d$' | head -1)
    if [ -z "$bin" ]; then
        echo "Error: binary for $test_name not found"
        exit 1
    fi

    echo "==> Uploading $(basename "$bin") to VM..."
    multipass exec "$VM" -- sudo rm -f "/home/ubuntu/$test_name"
    multipass transfer "$bin" "$VM:/home/ubuntu/$test_name"
    multipass exec "$VM" -- chmod +x "/home/ubuntu/$test_name"

    echo "==> Running $test_name in VM..."
    multipass exec "$VM" -- sudo env "${env_args[@]}" "/home/ubuntu/$test_name"
}

build_and_run vm \
    "BCR_BTRFS_MOUNT=$BTRFS_MOUNT"

build_and_run e2e \
    "BCR_BTRFS_MOUNT=$BTRFS_MOUNT" \
    "BCR_S3_ENDPOINT=$S3_ENDPOINT" \
    "BCR_S3_BUCKET=$S3_BUCKET" \
    "AWS_ACCESS_KEY_ID=$S3_USER" \
    "AWS_SECRET_ACCESS_KEY=$S3_PASS" \
    "AWS_DEFAULT_REGION=us-east-1"
