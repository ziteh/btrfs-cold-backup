#!/usr/bin/env bash
# Power-loss drill: hard-resets the test VM (sysrq, no sync) mid-backup, once while parts are
# being encrypted and once while they are being uploaded, then checks that resuming finishes
# the backup and that it restores byte-identical.
#
# Not part of `cargo test`: it reboots the VM. Run it after scripts/run-vm-tests.sh (which
# sets up the VM and copies the bcb binary there), before a release.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM=bcb-dev

vm() { multipass exec "$VM" -- sudo bash -c "$1"; }

wait_for_vm() {
    for _ in $(seq 1 60); do
        multipass exec "$VM" -- true 2>/dev/null && return
        sleep 3
    done
    echo "VM did not come back" >&2
    exit 1
}

# Runs in the VM: shared settings for every step below.
PRELUDE='
set -euo pipefail
D=/var/lib/bcb-drill
M=/mnt/bcb-test/drill
SNAP=bcb_level0.20260601T0000
BIN=/home/ubuntu/bcb
CFG=$D/config.yaml
export AWS_ACCESS_KEY_ID=s3testcreds AWS_SECRET_ACCESS_KEY=s3testcreds AWS_DEFAULT_REGION=us-east-1
export BCB_TEST_SPLIT_SIZE=$((16 * 1024 * 1024))
'

setup() {
    vm "$PRELUDE"'
    rm -rf "$D"; mkdir -p "$D"
    if [ -d "$M" ]; then
        # Every btrfs subvolume root has inode 256; deepest first, so nested ones go first.
        find "$M" -mindepth 1 -inum 256 -type d | sort -r | while read -r p; do
            btrfs subvolume delete "$p" >/dev/null
        done
        rm -rf "$M"
    fi
    mkdir -p "$M/snaps" "$M/restore"
    btrfs subvolume create "$M/data" >/dev/null
    head -c $((150 * 1024 * 1024)) /dev/urandom > "$M/data/big.bin"
    for i in $(seq 1 200); do head -c $((RANDOM * 4)) /dev/urandom > "$M/data/small-$i"; done
    btrfs subvolume snapshot -r "$M/data" "$M/snaps/$SNAP" >/dev/null
    pub=$("$BIN" genkey --output "$D/key" | grep -o "age1[0-9a-z]*" | head -1)
    cat > "$CFG" <<EOF
base_dir: $D/base
age_public_key: $pub
s3:
  bucket: bcb-test
  prefix: power-loss-drill-$(date +%s)/
  endpoint: http://localhost:8333
  region: us-east-1
  upload_speed_limit_kbps: 40000
  storage_class:
    manifest: STANDARD
    backup_data: [STANDARD]
tasks:
  - name: drill
    description: power-loss drill
    subvolume: $M/data
    snapshot_dir: $M/snaps
EOF
    sync'
}

# Starts a backup in the VM, then hard-resets the VM once `condition` holds. The reset drops
# the exec session without ever ending it, so that session runs in the background and is
# abandoned once the VM stops answering.
crash_when() {
    local condition=$1
    vm "$PRELUDE"'
    # A systemd unit, because a background child of `multipass exec` dies with the session.
    systemctl reset-failed bcb-drill-backup &>/dev/null || true
    systemd-run --unit=bcb-drill-backup --setenv=BCB_TEST_SPLIT_SIZE="$BCB_TEST_SPLIT_SIZE" \
        --setenv=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
        --setenv=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
        --setenv=AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
        "$BIN" --config "$CFG" backup --level 0 --task drill >/dev/null
    echo 1 > /proc/sys/kernel/sysrq
    for _ in $(seq 1 1200); do
        if '"$condition"'; then
            echo b > /proc/sysrq-trigger
        fi
        sleep 0.1
    done
    echo "crash condition never met" >&2
    exit 1' &
    local session=$!
    # The VM going down is what ends this wait; the session itself never returns.
    sleep 15
    for _ in $(seq 1 600); do
        multipass exec "$VM" -- true &>/dev/null || break
        sleep 0.5
    done
    kill "$session" 2>/dev/null || true
    wait "$session" 2>/dev/null || true
    wait_for_vm
    ensure_vm_ready
}

# A store that lost an acknowledged part makes the first resume fail on purpose, so allow
# one retry; anything beyond that is a failure.
resume_and_restore() {
    vm "$PRELUDE"'
    for attempt in 1 2; do
        if "$BIN" --config "$CFG" backup --level 0 --task drill; then break; fi
        [ "$attempt" = 2 ] && exit 1
    done
    "$BIN" --config "$CFG" restore --task drill --level 0 --date 202606010000 \
        --target "$M/restore" --private-key "$D/key"
    diff -r "$M/snaps/$SNAP" "$M/restore/$SNAP"
    echo "restored snapshot matches"'
}

ensure_vm_ready() {
    multipass transfer "$SCRIPT_DIR/setup-vm.sh" "$VM:setup-vm.sh"
    # Piped rather than sent to /dev/null: `multipass exec` busy-loops on a /dev/null stdout.
    multipass exec "$VM" -- sudo bash setup-vm.sh | tail -1
}

ensure_vm_ready

echo "==> Drill 1: power loss while encrypting"
setup
crash_when 'ls $D/base/backup/*/*/snapshot.part-000002.age >/dev/null 2>&1'
resume_and_restore

echo "==> Drill 2: power loss while uploading"
setup
crash_when 'grep -q "^- " $D/base/backup/*/state.yaml 2>/dev/null'
resume_and_restore

echo "==> Power-loss drills passed"
