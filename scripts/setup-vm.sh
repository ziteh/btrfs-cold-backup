#!/usr/bin/env bash
set -euo pipefail

LOOP_IMG=/var/lib/bcr-test/btrfs.img
BTRFS_MOUNT=/mnt/bcr-test
MINIO_DATA=/var/lib/minio
MINIO_BIN=/usr/local/bin/minio
MC_BIN=/usr/local/bin/mc
S3_BUCKET=bcr-test
S3_USER=minioadmin
S3_PASS=minioadmin

apt-get update -q
apt-get install -y btrfs-progs awscli curl

if ! command -v cargo &>/dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
fi

# btrfs loop device
mkdir -p "$(dirname "$LOOP_IMG")" "$BTRFS_MOUNT"
if ! mountpoint -q "$BTRFS_MOUNT"; then
    if [ ! -f "$LOOP_IMG" ]; then
        dd if=/dev/zero of="$LOOP_IMG" bs=1M count=1024 status=none
        mkfs.btrfs -q "$LOOP_IMG"
    fi
    mount -o loop "$LOOP_IMG" "$BTRFS_MOUNT"
fi

# MinIO server
if [ ! -f "$MINIO_BIN" ]; then
    curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o "$MINIO_BIN"
    chmod +x "$MINIO_BIN"
fi

if ! pgrep -x minio &>/dev/null; then
    mkdir -p "$MINIO_DATA"
    MINIO_ROOT_USER=$S3_USER MINIO_ROOT_PASSWORD=$S3_PASS \
        nohup "$MINIO_BIN" server "$MINIO_DATA" --address :9000 &>/var/log/minio.log &
    sleep 2
fi

# MinIO client + bucket
if [ ! -f "$MC_BIN" ]; then
    curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o "$MC_BIN"
    chmod +x "$MC_BIN"
fi
"$MC_BIN" alias set local http://localhost:9000 "$S3_USER" "$S3_PASS" &>/dev/null
"$MC_BIN" mb --ignore-existing "local/$S3_BUCKET"

echo "btrfs mount : $BTRFS_MOUNT"
echo "MinIO bucket: $S3_BUCKET (http://localhost:9000)"
echo ""
echo "Run Layer 3 (btrfs driver):"
echo "  BCR_BTRFS_MOUNT=$BTRFS_MOUNT sudo -E cargo test --test vm"
echo ""
echo "Run Layer 4 (e2e):"
echo "  BCR_BTRFS_MOUNT=$BTRFS_MOUNT \\"
echo "  BCR_S3_ENDPOINT=http://localhost:9000 \\"
echo "  BCR_S3_BUCKET=$S3_BUCKET \\"
echo "  AWS_ACCESS_KEY_ID=$S3_USER \\"
echo "  AWS_SECRET_ACCESS_KEY=$S3_PASS \\"
echo "  sudo -E cargo test --test e2e"
