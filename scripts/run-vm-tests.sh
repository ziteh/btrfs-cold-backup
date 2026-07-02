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

if ! multipass exec "$VM" -- bash -c "mountpoint -q '$BTRFS_MOUNT' && pgrep -x minio > /dev/null" 2>/dev/null; then
    echo "==> Running VM setup..."
    multipass transfer "$SCRIPT_DIR/setup-vm.sh" "$VM:setup-vm.sh"
    multipass exec "$VM" -- sudo bash setup-vm.sh
fi

# Host toolchain

if ! command -v cargo-zigbuild &>/dev/null; then
    echo "==> Installing cargo-zigbuild..."
    cargo install cargo-zigbuild
fi

rustup target add "$TARGET"

# Build and transfer the real bcb binary (dev profile, so debug_assertions stays true and the
# BCB_TEST_PAUSE_* hooks used by the interruption tests are compiled in).

echo "==> Building bcb binary..."
cargo zigbuild --bin bcb --target "$TARGET"
multipass exec "$VM" -- sudo rm -f /home/ubuntu/bcb
multipass transfer "target/$TARGET/debug/bcb" "$VM:/home/ubuntu/bcb"
multipass exec "$VM" -- chmod +x /home/ubuntu/bcb

# Build and run

build_and_run() {
    local test_name=$1
    shift
    local env_args=("$@")

    echo "==> Building $test_name..."
    cargo zigbuild --test "$test_name" --target "$TARGET"

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
    "BCB_BTRFS_MOUNT=$BTRFS_MOUNT" \
    "BCB_S3_ENDPOINT=$S3_ENDPOINT" \
    "BCB_S3_BUCKET=$S3_BUCKET" \
    "BCB_BIN_PATH=/home/ubuntu/bcb" \
    "AWS_ACCESS_KEY_ID=$S3_USER" \
    "AWS_SECRET_ACCESS_KEY=$S3_PASS" \
    "AWS_DEFAULT_REGION=us-east-1"

build_and_run e2e \
    "BCB_BTRFS_MOUNT=$BTRFS_MOUNT" \
    "BCB_S3_ENDPOINT=$S3_ENDPOINT" \
    "BCB_S3_BUCKET=$S3_BUCKET" \
    "AWS_ACCESS_KEY_ID=$S3_USER" \
    "AWS_SECRET_ACCESS_KEY=$S3_PASS" \
    "AWS_DEFAULT_REGION=us-east-1"
