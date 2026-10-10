#!/usr/bin/env bash
# Real-AWS drill: backs up to and restores from a real S3 bucket with archive storage classes,
# which neither the integration tests (mocked S3) nor the VM tests (SeaweedFS, STANDARD only)
# exercise. See docs/aws-drill.md for the one-time AWS setup.
#
# A Deep Archive retrieval takes about 12 hours, so the drill runs in two phases and, like
# `bcb restore`, is meant to be re-run until it exits 0:
#   - no run waiting on retrieval: start one (backups, assertions, retrieval requests) -> exit 2
#   - a run waiting on retrieval: finish its restores -> exit 2 while retrieval is running,
#     0 once both levels are restored and match their snapshots
# `--new` abandons a waiting run. Any failed assertion exits 1.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM=${VM:-bcb-aws}
# shellcheck source=vm-lib.sh
source "$SCRIPT_DIR/vm-lib.sh"

BUCKET=${BCB_DRILL_BUCKET:?set BCB_DRILL_BUCKET to the drill bucket (see docs/aws-drill.md)}
PROFILE=${BCB_DRILL_PROFILE:-bcb-drill}
NO_TAG_PROFILE=${BCB_DRILL_NO_TAG_PROFILE:-bcb-drill-no-tag}
RESTORE_PROFILE=${BCB_DRILL_RESTORE_PROFILE:-bcb-drill-restore}

# Runs in the VM before every script below.
PRELUDE='
set -euo pipefail
D=/var/lib/bcb-aws-drill
M=/mnt/bcb-test/aws-drill
BIN=/home/ubuntu/bcb
CFG=$D/config.yaml
SNAP0=bcb_level0.20260601T0000
SNAP1=bcb_level1.20260601T0100
DATE0=202606010000
DATE1=202606010100
export AWS_DEFAULT_REGION=us-east-1 RUST_LOG=info NO_COLOR=1
# Larger than one multipart chunk, so each part is uploaded in several chunks.
export BCB_TEST_SPLIT_SIZE=$((80 * 1024 * 1024))
if [ -f "$D/run.env" ]; then . "$D/run.env"; fi

LAST_LOG=
check() {
    local desc=$1
    shift
    if "$@"; then
        echo "  ok: $desc"
        return
    fi
    echo "FAIL: $desc" >&2
    if [ -n "$LAST_LOG" ]; then tail -n 30 "$LAST_LOG" >&2; fi
    exit 1
}

# Runs bcb with stderr logged to $D/logs/<name>.log, setting rc to its exit code.
bcb_run() {
    local name=$1
    shift
    LAST_LOG=$D/logs/$name.log
    rc=0
    "$BIN" --config "$CFG" "$@" 2>"$LAST_LOG" || rc=$?
}

# <log name> <level> <date> <target> <private key>
restore() {
    bcb_run "$1" restore --task drill --level "$2" --date "$3" --target "$4" --private-key "$5"
}

requested_retrieval() { grep -q "requesting retrieval" "$LAST_LOG"; }
requested_no_retrieval() { ! requested_retrieval; }

# The keys and storage classes of one level'\''s parts under $PREFIX, one "key class" per line.
part_objects() {
    aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "${PREFIX}backup-data/" \
        --query "Contents[?contains(Key, \`bcb_level$1_\`)].[Key, StorageClass]" --output text
}

level_tag() {
    aws s3api get-object-tagging --bucket "$BUCKET" --key "$1" \
        --query "TagSet[?Key==\`bcb-backup-level\`].Value" --output text
}

# <level> <storage class> <minimum part count>
check_parts() {
    local n=0 key class
    while read -r key class; do
        check "$key is $2" [ "$class" = "$2" ]
        check "$key is tagged level $1" [ "$(level_tag "$key")" = "$1" ]
        n=$((n + 1))
    done < <(part_objects "$1")
    check "level $1 has at least $3 parts (has $n)" [ "$n" -ge "$3" ]
}

check_manifest() {
    local key
    key=$(aws s3api list-objects-v2 --bucket "$BUCKET" --prefix "${PREFIX}manifests/" \
        --query "Contents[?contains(Key, \`bcb_level$1_\`)].Key" --output text)
    check "level $1 manifest is STANDARD" [ "$(aws s3api head-object --bucket "$BUCKET" \
        --key "$key" --query "StorageClass || \`STANDARD\`" --output text)" = STANDARD ]
    check "level $1 manifest is tagged manifest" [ "$(level_tag "$key")" = manifest ]
}

pending_multipart_uploads() {
    aws s3api list-multipart-uploads --bucket "$BUCKET" --prefix "$PREFIX" \
        --query "length(Uploads || \`[]\`)" --output text
}

reset_drill_dirs() {
    if [ -d "$M" ]; then
        # Every btrfs subvolume root has inode 256; deepest first, so nested ones go first.
        find "$M" -mindepth 1 -inum 256 -type d | sort -r | while read -r p; do
            btrfs subvolume delete "$p" >/dev/null
        done
        rm -rf "$M"
    fi
    rm -rf "$D"
}
'

# Runs a script, read from stdin, as root in the VM with a profile's credentials. Everything
# goes over stdin so the credentials never appear in a command line or a file in the VM. The
# script is wrapped in braces so bash reads all of it before running any of it; otherwise a
# command that reads stdin would swallow the rest of the script.
vm_run() {
    local profile=$1 script creds
    script=$(cat)
    creds=$(aws configure export-credentials --profile "$profile" --format env)
    printf '{\n%s\nBUCKET=%q\nNEW_PREFIX=%q\n%s\n%s\n}\n' \
        "$creds" "$BUCKET" "${NEW_PREFIX:-}" "$PRELUDE" "$script" |
        multipass exec "$VM" -- sudo bash -s | cat
    return "${PIPESTATUS[1]}"
}

prepare_vm() {
    for cmd in aws jq multipass cargo-zigbuild; do
        command -v "$cmd" >/dev/null || {
            echo "$cmd is required on the host" >&2
            exit 1
        }
    done
    # Checked before the build, which takes minutes, rather than at first use after it.
    for profile in "$PROFILE" "$NO_TAG_PROFILE" "$RESTORE_PROFILE"; do
        if ! aws configure export-credentials --profile "$profile" >/dev/null; then
            echo "AWS profile '$profile' is not usable; see docs/aws-drill.md" >&2
            exit 1
        fi
    done
    # The drill's lifecycle rules expire what it writes; they must never share a bucket with
    # backups someone depends on.
    local foreign
    foreign=$(aws s3api list-objects-v2 --bucket "$BUCKET" --delimiter / --profile "$PROFILE" \
        --output json |
        jq -r '[.CommonPrefixes[]?.Prefix, .Contents[]?.Key] | map(select(. != "aws-drill/")) | .[]')
    if [ -n "$foreign" ]; then
        echo "s3://$BUCKET holds data that is not from the drill; use a dedicated bucket:" >&2
        echo "$foreign" | head -5 >&2
        exit 1
    fi
    if ! multipass info "$VM" &>/dev/null; then
        echo "==> Launching VM $VM..."
        multipass launch --name "$VM" --disk 20G --memory 2G --cpus 2
    fi
    vm_setup --no-s3 | tail -1

    local target
    target=$(vm_target)
    rustup target add "$target" >/dev/null
    # The dev profile keeps the BCB_TEST_* hooks the drill drives; the release build is what
    # production runs.
    echo "==> Building bcb (dev and release)..."
    cargo zigbuild --bin bcb --target "$target"
    cargo zigbuild --release --bin bcb --target "$target"
    vm_upload "target/$target/debug/bcb" bcb
    vm_upload "target/$target/release/bcb" bcb-release
}

start_run() {
    NEW_PREFIX="aws-drill/run-$(date -u +%Y%m%dT%H%M%S)-$(git -C "$SCRIPT_DIR" rev-parse --short HEAD)/"
    echo "==> Starting a new drill run under s3://$BUCKET/$NEW_PREFIX"
    prepare_vm

    echo "==> Setting up the test subvolume and config"
    vm_run "$PROFILE" <<'EOF'
reset_drill_dirs
mkdir -p "$D/logs" "$M/snaps" "$M/restore" "$M/seed"
printf 'PREFIX=%q\n' "$NEW_PREFIX" >"$D/run.env"
PREFIX=$NEW_PREFIX
btrfs subvolume create "$M/data" >/dev/null
head -c $((200 * 1024 * 1024)) /dev/urandom >"$M/data/big.bin"
for i in $(seq 1 100); do head -c $((RANDOM * 4)) /dev/urandom >"$M/data/small-$i"; done
btrfs subvolume snapshot -r "$M/data" "$M/snaps/$SNAP0" >/dev/null
pub=$("$BIN" genkey --output "$D/key" | grep -o "age1[0-9a-z]*" | head -1)
"$BIN" genkey --output "$D/wrong.key" >/dev/null
cat >"$CFG" <<CONFIG
base_dir: $D/base
age_public_key: $pub
s3:
  bucket: $BUCKET
  prefix: $PREFIX
  endpoint: ""
  region: us-east-1
  storage_class:
    manifest: STANDARD
    backup_data: [DEEP_ARCHIVE, GLACIER]
  restore:
    # Large enough that one wave covers every part: waiting on several waves of Deep
    # Archive retrieval would take days, and the wave logic is covered by mocked tests.
    expected_download_kbps: 100000
tasks:
  - name: drill
    description: real AWS drill
    subvolume: $M/data
    snapshot_dir: $M/snaps
CONFIG
EOF

    echo "==> Checking permissions"
    vm_run "$PROFILE" <<'EOF'
bcb_run check check
check "bcb check passes with the production policy" [ "$rc" = 0 ]
EOF
    vm_run "$NO_TAG_PROFILE" <<'EOF'
bcb_run check-no-tag check
check "bcb check fails without s3:PutObjectTagging" [ "$rc" != 0 ]
check "it fails at the write probe" grep -q "cannot write to S3 bucket" "$LAST_LOG"
EOF

    echo "==> Level 0 backup, killed mid-upload and resumed"
    vm_run "$PROFILE" <<'EOF'
LAST_LOG=$D/logs/backup0-killed.log
BCB_TEST_PAUSE_AFTER_S3_PART_NUMBER=1 "$BIN" --config "$CFG" backup --level 0 2>"$LAST_LOG" &
pid=$!
for _ in $(seq 1 600); do
    if grep -q "BCB_TEST_PAUSE_REACHED:s3_part:1" "$LAST_LOG"; then break; fi
    sleep 1
done
check "backup paused after the first chunk" grep -q "BCB_TEST_PAUSE_REACHED:s3_part:1" "$LAST_LOG"
kill -9 "$pid"
wait "$pid" || true
check "the killed backup left a multipart upload" [ "$(pending_multipart_uploads)" -ge 1 ]

bcb_run backup0 backup --level 0
check "the resumed backup succeeds" [ "$rc" = 0 ]
check "it resumed the unfinished backup" grep -q "resuming the unfinished" "$LAST_LOG"
check "no multipart upload is left" [ "$(pending_multipart_uploads)" = 0 ]
check_parts 0 DEEP_ARCHIVE 3
check_manifest 0

bcb_run backup0-again backup --level 0
check "a repeated backup succeeds" [ "$rc" = 0 ]
check "it skips the existing backup" grep -q "already backed up at level 0, skipping" "$LAST_LOG"
EOF

    echo "==> Level 1 backup"
    vm_run "$PROFILE" <<'EOF'
head -c $((20 * 1024 * 1024)) /dev/urandom >"$M/data/delta.bin"
rm -f "$M/data/small-1"
btrfs subvolume snapshot -r "$M/data" "$M/snaps/$SNAP1" >/dev/null
bcb_run backup1 backup --level 1
check "the level 1 backup succeeds" [ "$rc" = 0 ]
check_parts 1 GLACIER 1
check_manifest 1

bcb_run list list
check "bcb list succeeds" [ "$rc" = 0 ]
check "bcb list shows both levels" [ "$("$BIN" --config "$CFG" list 2>/dev/null | grep -c "^drill")" = 2 ]
bcb_run verify verify
check "bcb verify succeeds" [ "$rc" = 0 ]
EOF

    echo "==> Requesting retrieval"
    vm_run "$PROFILE" <<'EOF'
restore restore0-backup-policy 0 "$DATE0" "$M/restore" "$D/key"
check "a restore with the backup policy exits 1" [ "$rc" = 1 ]
check "it is denied the retrieval request" grep -q "restore_object failed" "$LAST_LOG"
EOF
    vm_run "$RESTORE_PROFILE" <<'EOF'
restore restore0-wrong-key 0 "$DATE0" "$M/restore" "$D/wrong.key"
check "a restore with the wrong key exits 1" [ "$rc" = 1 ]
check "it requests no retrieval" requested_no_retrieval

restore restore0-request 0 "$DATE0" "$M/restore" "$D/key"
check "the level 0 restore exits 2 (in progress)" [ "$rc" = 2 ]
check "it requests retrieval" requested_retrieval
restore restore0-repeat 0 "$DATE0" "$M/restore" "$D/key"
check "a repeated restore exits 2" [ "$rc" = 2 ]
check "it requests no retrieval again" requested_no_retrieval

# The level 1 restore checks that its parent is on the target before requesting any
# retrieval, so it is pointed at a copy of the level 0 snapshot received locally. Its state
# is keyed by backup, not target, so finishing onto the real target later picks it up.
btrfs send -q "$M/snaps/$SNAP0" | btrfs receive "$M/seed" >/dev/null
restore restore1-request 1 "$DATE1" "$M/seed" "$D/key"
check "the level 1 restore exits 2 (in progress)" [ "$rc" = 2 ]
check "it requests retrieval" requested_retrieval
btrfs subvolume delete "$M/seed/$SNAP0" >/dev/null

trap 'iptables -D OUTPUT -p tcp --dport 443 -j REJECT' EXIT
iptables -I OUTPUT -p tcp --dport 443 -j REJECT
restore restore0-offline 0 "$DATE0" "$M/restore" "$D/key"
check "a restore with the network cut exits 1" [ "$rc" = 1 ]
EOF

    echo "==> Release build smoke test"
    vm_run "$PROFILE" <<'EOF'
BIN=/home/ubuntu/bcb-release
CFG=$D/release.yaml
PREFIX=${PREFIX}release/
mkdir -p "$M/release-snaps"
btrfs subvolume create "$M/release-data" >/dev/null
head -c $((15 * 1024 * 1024)) /dev/urandom >"$M/release-data/payload.bin"
btrfs subvolume snapshot -r "$M/release-data" "$M/release-snaps/$SNAP0" >/dev/null
sed -e "s|^base_dir: .*|base_dir: $D/release-base|" \
    -e "s|^  prefix: .*|  prefix: $PREFIX|" \
    -e "s|^    subvolume: .*|    subvolume: $M/release-data|" \
    -e "s|^    snapshot_dir: .*|    snapshot_dir: $M/release-snaps|" \
    "$D/config.yaml" >"$CFG"
bcb_run release-check check
check "release bcb check passes" [ "$rc" = 0 ]
bcb_run release-backup0 backup --level 0
check "release level 0 backup succeeds" [ "$rc" = 0 ]
check_parts 0 DEEP_ARCHIVE 1
check_manifest 0
bcb_run release-list list
check "release bcb list succeeds" [ "$rc" = 0 ]
bcb_run release-verify verify
check "release bcb verify succeeds" [ "$rc" = 0 ]

echo "PHASE=waiting" >>"$D/run.env"
EOF

    echo "==> Retrieval requested. Re-run this in about 12 hours to finish the restores."
    exit 2
}

finish_run() {
    echo "==> Finishing the waiting drill run"
    local rc=0
    vm_run "$RESTORE_PROFILE" <<'EOF' || rc=$?
# A completed restore removes its work dir, so running it again would start over; each
# level's completion is recorded instead.
for level in 0 1; do
    done_var=DONE$level date_var=DATE$level
    if [ -n "${!done_var:-}" ]; then continue; fi
    restore "finish$level" "$level" "${!date_var}" "$M/restore" "$D/key"
    case "$rc" in
        0) echo "DONE$level=1" >>"$D/run.env" ;;
        2) echo "level $level: retrieval still in progress"; exit 2 ;;
        *) check "the level $level restore succeeds" false ;;
    esac
done
check "level 0 restores byte-identical" diff -r "$M/snaps/$SNAP0" "$M/restore/$SNAP0"
check "level 1 restores byte-identical" diff -r "$M/snaps/$SNAP1" "$M/restore/$SNAP1"
reset_drill_dirs
EOF
    case "$rc" in
        0) echo "==> AWS drill passed" ;;
        2) echo "==> Still waiting on retrieval; re-run later." ;;
    esac
    exit "$rc"
}

new=false
for arg in "$@"; do
    case "$arg" in
        --new) new=true ;;
        *)
            echo "unknown option: $arg" >&2
            exit 1
            ;;
    esac
done

if ! $new && multipass info "$VM" &>/dev/null &&
    vm_exec 'grep -qx PHASE=waiting /var/lib/bcb-aws-drill/run.env 2>/dev/null'; then
    finish_run
fi
start_run
