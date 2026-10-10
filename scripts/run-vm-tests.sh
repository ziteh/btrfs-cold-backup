#!/usr/bin/env bash
# Runs the VM test suite (tests/vm) in a Multipass VM with a real btrfs and S3-compatible
# server, creating and setting up the VM on first use. `--fresh` recreates the VM.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=vm-lib.sh
source "$SCRIPT_DIR/vm-lib.sh"
BTRFS_MOUNT=/mnt/bcb-test
S3_ENDPOINT=http://localhost:8333
S3_BUCKET=bcb-test
S3_USER=s3testcreds
S3_PASS=s3testcreds

# Reset every run: the VM is long-lived, and state left by earlier runs (a full disk, stray
# fixtures) otherwise surfaces as failures that look like bcb bugs. `--fresh` also recreates
# the VM itself, to rule out the environment entirely.
if [ "${1:-}" = "--fresh" ]; then
    multipass delete --purge "$VM" 2>/dev/null || true
fi
if ! multipass info "$VM" &>/dev/null; then
    echo "==> Launching VM $VM..."
    # SeaweedFS runs master, volume, filer and S3 in one process; on multipass's default
    # memory it is OOM-killed partway through the suite, which surfaces as unrelated-looking
    # "dispatch failure" errors from every S3 call after that point.
    multipass launch --name "$VM" --disk 20G --memory 4G --cpus 2
fi

echo "==> Resetting VM test state..."
vm_setup --reset

if ! command -v cargo-zigbuild &>/dev/null; then
    echo "==> Installing cargo-zigbuild..."
    cargo install cargo-zigbuild
fi

TARGET=$(vm_target)
rustup target add "$TARGET"

# The dev profile keeps debug_assertions, which the BCB_TEST_* hooks the VM tests drive
# depend on.
echo "==> Building bcb and the VM tests..."
cargo zigbuild --bin bcb --test vm --target "$TARGET"
test_bin=$(ls -t target/"$TARGET"/debug/deps/vm-* 2>/dev/null | grep -v '\.d$' | head -1)
if [ -z "$test_bin" ]; then
    echo "Error: VM test binary not found"
    exit 1
fi

echo "==> Uploading to VM..."
vm_upload "target/$TARGET/debug/bcb" bcb
vm_upload "$test_bin" vm-tests

echo "==> Running VM tests..."
multipass exec "$VM" -- sudo env \
    "BCB_BTRFS_MOUNT=$BTRFS_MOUNT" \
    "BCB_S3_ENDPOINT=$S3_ENDPOINT" \
    "BCB_S3_BUCKET=$S3_BUCKET" \
    "BCB_BIN_PATH=/home/ubuntu/bcb" \
    "AWS_ACCESS_KEY_ID=$S3_USER" \
    "AWS_SECRET_ACCESS_KEY=$S3_PASS" \
    "AWS_DEFAULT_REGION=us-east-1" \
    /home/ubuntu/vm-tests
