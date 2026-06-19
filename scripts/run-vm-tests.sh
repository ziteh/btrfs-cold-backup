#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM=bcb-dev
BTRFS_MOUNT=/mnt/bcb-test
S3_ENDPOINT=http://localhost:9000
S3_BUCKET=bcb-test
S3_USER=minioadmin
S3_PASS=minioadmin
TARGET=aarch64-unknown-linux-gnu

# VM setup

if ! multipass info "$VM" &>/dev/null; then
    echo "==> Launching VM $VM..."
    multipass launch --name "$VM" --disk 10G
fi

if ! multipass exec "$VM" -- mountpoint -q "$BTRFS_MOUNT" &>/dev/null || \
   ! multipass exec "$VM" -- pgrep -x minio &>/dev/null; then
    echo "==> Running VM setup..."
    multipass transfer "$SCRIPT_DIR/setup-vm.sh" "$VM:setup-vm.sh"
    multipass exec "$VM" -- sudo bash setup-vm.sh
fi

# Host toolchain

if ! command -v cross &>/dev/null; then
    echo "==> Installing cross..."
    cargo install cross
fi

rustup target add "$TARGET"

# Build and run

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
    "BCB_BTRFS_MOUNT=$BTRFS_MOUNT"

build_and_run e2e \
    "BCB_BTRFS_MOUNT=$BTRFS_MOUNT" \
    "BCB_S3_ENDPOINT=$S3_ENDPOINT" \
    "BCB_S3_BUCKET=$S3_BUCKET" \
    "AWS_ACCESS_KEY_ID=$S3_USER" \
    "AWS_SECRET_ACCESS_KEY=$S3_PASS" \
    "AWS_DEFAULT_REGION=us-east-1"
