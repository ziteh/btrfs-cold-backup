#!/usr/bin/env bash
set -euo pipefail

LOOP_IMG=/var/lib/bcb-test/btrfs.img
BTRFS_MOUNT=/mnt/bcb-test
MINIO_DATA=/var/lib/minio
MINIO_BIN=/usr/local/bin/minio
MC_BIN=/usr/local/bin/mc
S3_BUCKET=bcb-test
S3_USER=minioadmin
S3_PASS=minioadmin

# Sync clock before apt
chronyc makestep 2>/dev/null || true

apt-get update -q
apt-get install -y btrfs-progs curl

# btrfs loop device
mkdir -p "$(dirname "$LOOP_IMG")" "$BTRFS_MOUNT"
if ! mountpoint -q "$BTRFS_MOUNT"; then
    if [ ! -f "$LOOP_IMG" ]; then
        dd if=/dev/zero of="$LOOP_IMG" bs=1M count=1024 status=none
        mkfs.btrfs -q "$LOOP_IMG"
    fi
    mount -o loop "$LOOP_IMG" "$BTRFS_MOUNT"
fi

# Detect architecture for MinIO downloads
case "$(uname -m)" in
    aarch64|arm64) ARCH=arm64 ;;
    *)             ARCH=amd64 ;;
esac

# MinIO server
if [ ! -f "$MINIO_BIN" ] || ! file "$MINIO_BIN" | grep -q "$(uname -m)"; then
    curl -fsSL "https://dl.min.io/server/minio/release/linux-${ARCH}/minio" -o "$MINIO_BIN"
    chmod +x "$MINIO_BIN"
fi

if ! pgrep -x minio &>/dev/null; then
    mkdir -p "$MINIO_DATA"
    MINIO_ROOT_USER=$S3_USER MINIO_ROOT_PASSWORD=$S3_PASS \
        nohup "$MINIO_BIN" server "$MINIO_DATA" --address :9000 &>/var/log/minio.log &
    sleep 2
fi

# MinIO client + bucket
if [ ! -f "$MC_BIN" ] || ! file "$MC_BIN" | grep -q "$(uname -m)"; then
    curl -fsSL "https://dl.min.io/client/mc/release/linux-${ARCH}/mc" -o "$MC_BIN"
    chmod +x "$MC_BIN"
fi
"$MC_BIN" alias set local http://localhost:9000 "$S3_USER" "$S3_PASS" &>/dev/null
"$MC_BIN" mb --ignore-existing "local/$S3_BUCKET"

echo "VM ready."
echo "btrfs mount : $BTRFS_MOUNT"
echo "MinIO bucket: $S3_BUCKET @ http://localhost:9000"
