#!/usr/bin/env bash
# Sets up the test environment the VM tests need: a btrfs loopback mount and a local
# S3-compatible server (SeaweedFS) with the test bucket. Runs as root inside the test VM, or
# on a CI runner. Safe to re-run.
set -euo pipefail

LOOP_IMG=/var/lib/bcb-test/btrfs.img
BTRFS_MOUNT=/mnt/bcb-test
SEAWEEDFS_DATA=/var/lib/seaweedfs
SEAWEEDFS_BIN=/usr/local/bin/weed
S3_BUCKET=bcb-test
S3_PORT=8333
# Dummy credentials: SeaweedFS's S3 gateway runs anonymous (no -s3.config) and doesn't
# validate them, but the AWS SDK still requires some value to sign requests with.
S3_USER=s3testcreds
S3_PASS=s3testcreds

# `--reset` throws away the test state (btrfs image, S3 data) before setting up, so a run
# doesn't inherit leftovers of earlier ones: a full disk, stray fixtures, objects under old
# prefixes. Left off by default because the power-loss drill re-runs this script after a
# reboot, and CI starts from a clean machine anyway.
if [ "${1:-}" = "--reset" ]; then
    systemctl stop bcb-weed &>/dev/null || true
    pkill -x weed || true
    while pgrep -x weed &>/dev/null; do sleep 0.5; done
    mountpoint -q "$BTRFS_MOUNT" && umount "$BTRFS_MOUNT"
    rm -f "$LOOP_IMG"
    rm -rf "$SEAWEEDFS_DATA"
fi

# An automatic upgrade interrupted by a crash leaves packages half-configured, and apt then
# refuses everything until repaired.
systemctl disable --now unattended-upgrades apt-daily.timer apt-daily-upgrade.timer &>/dev/null || true

# apt rejects repository metadata dated in the future, which a lagging VM clock makes it.
chronyc makestep 2>/dev/null || true

# Skipped once installed: re-running after a VM crash shouldn't depend on apt, which a crash
# mid-upgrade can leave broken.
if ! command -v btrfs &>/dev/null || ! command -v curl &>/dev/null || ! command -v unzip &>/dev/null; then
    apt-get update -q
    apt-get install -y btrfs-progs curl unzip
fi

# AWS CLI v2 (used only to create the test bucket below). Not installed via apt: Ubuntu
# noble dropped the `awscli` package, and GitHub-hosted runners already have v2 preinstalled
# anyway, so this only triggers on a fresh local VM.
if ! command -v aws &>/dev/null; then
    case "$(uname -m)" in
        aarch64|arm64) awscli_arch=aarch64 ;;
        *)             awscli_arch=x86_64 ;;
    esac
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${awscli_arch}.zip" -o /tmp/awscliv2.zip
    unzip -q -o /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install
    rm -rf /tmp/awscliv2.zip /tmp/aws
fi

mkdir -p "$(dirname "$LOOP_IMG")" "$BTRFS_MOUNT"
if ! mountpoint -q "$BTRFS_MOUNT"; then
    if [ ! -f "$LOOP_IMG" ]; then
        dd if=/dev/zero of="$LOOP_IMG" bs=1M count=1024 status=none
        mkfs.btrfs -q "$LOOP_IMG"
    fi
    mount -o loop "$LOOP_IMG" "$BTRFS_MOUNT"
fi

if [ ! -f "$SEAWEEDFS_BIN" ]; then
    case "$(uname -m)" in
        aarch64|arm64) weed_arch=arm64 ;;
        *)             weed_arch=amd64 ;;
    esac
    # Piping curl straight into `grep -m1` risks curl seeing SIGPIPE (exit 23) when grep
    # quits after its first match, so capture the full response before grepping it.
    release_json=$(curl -fsSL https://api.github.com/repos/seaweedfs/seaweedfs/releases/latest)
    version=$(grep -m1 '"tag_name"' <<<"$release_json" | cut -d'"' -f4)
    curl -fsSL "https://github.com/seaweedfs/seaweedfs/releases/download/${version}/linux_${weed_arch}.tar.gz" \
        -o /tmp/weed.tar.gz
    tar -xzf /tmp/weed.tar.gz -C "$(dirname "$SEAWEEDFS_BIN")" weed
    rm -f /tmp/weed.tar.gz
    chmod +x "$SEAWEEDFS_BIN"
fi

if ! pgrep -x weed &>/dev/null; then
    mkdir -p "$SEAWEEDFS_DATA"
    # A systemd unit outlives the `multipass exec` session that started it, which a nohup'd
    # child does not. -volume.max is raised from the default 8 because volumes left
    # unwritable by a crash count against it, and once all are, every upload fails.
    systemctl reset-failed bcb-weed &>/dev/null || true
    systemd-run --unit=bcb-weed "$SEAWEEDFS_BIN" server -dir="$SEAWEEDFS_DATA" -s3 \
        -s3.port="$S3_PORT" -master.port=9333 -volume.port=8080 -filer.port=8888 \
        -volume.max=100 -ip=localhost
    for _ in $(seq 1 30); do
        curl -fsS "http://localhost:$S3_PORT" &>/dev/null && break
        sleep 1
    done
fi

AWS_ACCESS_KEY_ID=$S3_USER AWS_SECRET_ACCESS_KEY=$S3_PASS AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "http://localhost:$S3_PORT" s3 mb "s3://$S3_BUCKET" 2>/dev/null || true

echo "VM ready."
echo "btrfs mount      : $BTRFS_MOUNT"
echo "S3 (SeaweedFS) bucket: $S3_BUCKET @ http://localhost:$S3_PORT"
