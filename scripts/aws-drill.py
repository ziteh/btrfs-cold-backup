#!/usr/bin/env python3
"""Real-AWS drill: backs up to and restores from a real S3 bucket with archive storage classes,
which neither the integration tests (mocked S3) nor the VM tests (SeaweedFS, STANDARD only)
exercise. See docs/aws-drill.md for the one-time AWS setup.

A Deep Archive retrieval takes about 12 hours, so the drill runs in two phases and, like
`bcb restore`, is meant to be re-run until it exits 0:
  - no run waiting on retrieval: start one (backups, assertions, retrieval requests) -> exit 2
  - a run waiting on retrieval: finish its restores -> exit 2 while retrieval is running,
    0 once both levels are restored and match their snapshots
`--new` abandons a waiting run. Any failed assertion exits 1.

The same file runs on both sides. On the host it builds bcb and drives the VM; it also copies
itself into the VM, where `aws-drill.py guest <step>` runs one step as root.
"""

import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

MiB = 1024 * 1024

# Inside the VM.
DRILL_DIR = Path("/var/lib/bcb-aws-drill")
LOG_DIR = DRILL_DIR / "logs"
STATE_FILE = DRILL_DIR / "run.json"
KEY = DRILL_DIR / "key"
WRONG_KEY = DRILL_DIR / "wrong.key"
CFG = DRILL_DIR / "config.yaml"
RELEASE_CFG = DRILL_DIR / "release.yaml"
MNT = Path("/mnt/bcb-test/aws-drill")
DATA = MNT / "data"
SNAPS = MNT / "snaps"
RESTORE = MNT / "restore"
SEED = MNT / "seed"
RELEASE_DATA = MNT / "release-data"
RELEASE_SNAPS = MNT / "release-snaps"
BIN = Path("/home/ubuntu/bcb")
RELEASE_BIN = Path("/home/ubuntu/bcb-release")
VM_SCRIPT = Path("/home/ubuntu/aws-drill.py")
SNAP0 = "bcb_level0.20260601T0000"
SNAP1 = "bcb_level1.20260601T0100"
DATE0 = "202606010000"
DATE1 = "202606010100"
SUBVOLUME_ROOT_INODE = 256

# On the host.
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent


def run(*cmd, **kwargs):
    return subprocess.run([str(c) for c in cmd], check=True, **kwargs)


def output(*cmd):
    return run(*cmd, stdout=subprocess.PIPE, text=True).stdout


def s3api(command, bucket, *args):
    out = output("aws", "s3api", command, "--bucket", bucket, *args, "--output", "json")
    # Some commands print nothing at all when there is nothing to list.
    return json.loads(out) if out.strip() else {}


# Guest: runs as root inside the VM, one step per invocation.
class Guest:
    """Drives one bcb binary and config against one S3 prefix, and remembers the log of the
    last bcb run so a failed check can print it."""

    def __init__(self, bucket, prefix, bcb_bin, cfg):
        self.bucket = bucket
        self.prefix = prefix
        self.bcb_bin = bcb_bin
        self.cfg = cfg
        self.last_log = None

    def check(self, desc, ok):
        if ok:
            print(f"  ok: {desc}")
            return
        print(f"FAIL: {desc}", file=sys.stderr)
        if self.last_log:
            sys.stderr.writelines(self.last_log.read_text().splitlines(keepends=True)[-30:])
        sys.exit(1)

    def bcb(self, log_name, *args):
        """Runs bcb with stderr logged to LOG_DIR/<log_name>.log and returns its exit code."""
        self.last_log = LOG_DIR / f"{log_name}.log"
        with self.last_log.open("w") as log:
            cmd = [self.bcb_bin, "--config", self.cfg, *args]
            return subprocess.run([str(c) for c in cmd], check=False, stderr=log).returncode

    def restore(self, log_name, level, date, target, key):
        return self.bcb(
            log_name,
            "restore",
            "--task",
            "drill",
            "--level",
            level,
            "--date",
            date,
            "--target",
            target,
            "--private-key",
            key,
        )

    def log_has(self, text):
        return text in self.last_log.read_text()

    def level_objects(self, folder, level):
        listing = s3api("list-objects-v2", self.bucket, "--prefix", f"{self.prefix}{folder}/")
        return [o for o in listing.get("Contents", []) if f"bcb_level{level}_" in o["Key"]]

    def level_tag(self, key):
        tags = s3api("get-object-tagging", self.bucket, "--key", key)["TagSet"]
        return next((t["Value"] for t in tags if t["Key"] == "bcb-backup-level"), None)

    def check_parts(self, level, storage_class, min_count):
        parts = self.level_objects("backup-data", level)
        for part in parts:
            key = part["Key"]
            self.check(f"{key} is {storage_class}", part["StorageClass"] == storage_class)
            self.check(f"{key} is tagged level {level}", self.level_tag(key) == str(level))
        self.check(
            f"level {level} has at least {min_count} parts (has {len(parts)})",
            len(parts) >= min_count,
        )

    def check_manifest(self, level):
        [manifest] = self.level_objects("manifests", level)
        key = manifest["Key"]
        # head-object leaves StorageClass out for STANDARD objects.
        head = s3api("head-object", self.bucket, "--key", key)
        self.check(
            f"level {level} manifest is STANDARD",
            head.get("StorageClass", "STANDARD") == "STANDARD",
        )
        self.check(
            f"level {level} manifest is tagged manifest",
            self.level_tag(key) == "manifest",
        )

    def pending_multipart_uploads(self):
        uploads = s3api("list-multipart-uploads", self.bucket, "--prefix", self.prefix)
        return len(uploads.get("Uploads", []))


def btrfs(*args):
    run("btrfs", *args, stdout=subprocess.DEVNULL)


def write_random(path, size):
    path.write_bytes(os.urandom(size))


def load_state():
    return json.loads(STATE_FILE.read_text())


def save_state(state):
    STATE_FILE.write_text(json.dumps(state))


def reset_drill_dirs():
    if MNT.exists():
        # Every btrfs subvolume root has the same fixed inode number. Deepest first, so nested
        # subvolumes are deleted before their parents.
        subvolumes = [
            Path(root)
            for root, _, _ in os.walk(MNT)
            if Path(root) != MNT and os.stat(root).st_ino == SUBVOLUME_ROOT_INODE
        ]
        for subvolume in sorted(subvolumes, reverse=True):
            btrfs("subvolume", "delete", subvolume)
        shutil.rmtree(MNT)
    if DRILL_DIR.exists():
        shutil.rmtree(DRILL_DIR)


def write_config(path, *, bucket, prefix, public_key, base_dir, subvolume, snapshot_dir):
    path.write_text(f"""\
base_dir: {base_dir}
age_public_key: {public_key}
s3:
  bucket: {bucket}
  prefix: {prefix}
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
    subvolume: {subvolume}
    snapshot_dir: {snapshot_dir}
""")


def setup(g):
    reset_drill_dirs()
    for d in (LOG_DIR, SNAPS, RESTORE, SEED):
        d.mkdir(parents=True)
    btrfs("subvolume", "create", DATA)
    write_random(DATA / "big.bin", 200 * MiB)
    for i in range(1, 101):
        write_random(DATA / f"small-{i}", random.randrange(128 * 1024))
    btrfs("subvolume", "snapshot", "-r", DATA, SNAPS / SNAP0)

    genkey_out = output(g.bcb_bin, "genkey", "--output", KEY)
    public_key = re.search(r"age1[0-9a-z]*", genkey_out).group()
    run(g.bcb_bin, "genkey", "--output", WRONG_KEY, stdout=subprocess.DEVNULL)
    write_config(
        CFG,
        bucket=g.bucket,
        prefix=g.prefix,
        public_key=public_key,
        base_dir=DRILL_DIR / "base",
        subvolume=DATA,
        snapshot_dir=SNAPS,
    )
    save_state({"prefix": g.prefix, "public_key": public_key, "done": []})


def check_permissions(g):
    rc = g.bcb("check", "check")
    g.check("bcb check passes with the production policy", rc == 0)


def check_no_tag(g):
    rc = g.bcb("check-no-tag", "check")
    g.check("bcb check fails without s3:PutObjectTagging", rc != 0)
    g.check("it fails at the write probe", g.log_has("cannot write to S3 bucket"))


def backup_level0(g):
    pause_mark = "BCB_TEST_PAUSE_REACHED:s3_part:1"
    g.last_log = LOG_DIR / "backup0-killed.log"
    with g.last_log.open("w") as log:
        backup = subprocess.Popen(
            [g.bcb_bin, "--config", g.cfg, "backup", "--level", "0"],
            stderr=log,
            env=os.environ | {"BCB_TEST_PAUSE_AFTER_S3_PART_NUMBER": "1"},
        )
    for _ in range(600):
        if g.log_has(pause_mark):
            break
        time.sleep(1)
    g.check("backup paused after the first chunk", g.log_has(pause_mark))
    backup.kill()
    backup.wait()
    g.check("the killed backup left a multipart upload", g.pending_multipart_uploads() >= 1)

    rc = g.bcb("backup0", "backup", "--level", 0)
    g.check("the resumed backup succeeds", rc == 0)
    g.check("it resumed the unfinished backup", g.log_has("resuming the unfinished"))
    g.check("no multipart upload is left", g.pending_multipart_uploads() == 0)
    g.check_parts(0, "DEEP_ARCHIVE", 3)
    g.check_manifest(0)

    rc = g.bcb("backup0-again", "backup", "--level", 0)
    g.check("a repeated backup succeeds", rc == 0)
    g.check(
        "it skips the existing backup",
        g.log_has("already backed up at level 0, skipping"),
    )


def backup_level1(g):
    write_random(DATA / "delta.bin", 20 * MiB)
    (DATA / "small-1").unlink()
    btrfs("subvolume", "snapshot", "-r", DATA, SNAPS / SNAP1)
    rc = g.bcb("backup1", "backup", "--level", 1)
    g.check("the level 1 backup succeeds", rc == 0)
    g.check_parts(1, "GLACIER", 1)
    g.check_manifest(1)

    rc = g.bcb("list", "list")
    g.check("bcb list succeeds", rc == 0)
    listed = output(g.bcb_bin, "--config", g.cfg, "list").splitlines()
    g.check(
        "bcb list shows both levels",
        len([line for line in listed if line.startswith("drill")]) == 2,
    )
    rc = g.bcb("verify", "verify")
    g.check("bcb verify succeeds", rc == 0)


def restore_denied(g):
    rc = g.restore("restore0-backup-policy", 0, DATE0, RESTORE, KEY)
    g.check("a restore with the backup policy exits 1", rc == 1)
    g.check("it is denied the retrieval request", g.log_has("restore_object failed"))


def request_retrieval(g):
    rc = g.restore("restore0-wrong-key", 0, DATE0, RESTORE, WRONG_KEY)
    g.check("a restore with the wrong key exits 1", rc == 1)
    g.check("it requests no retrieval", not g.log_has("requesting retrieval"))

    rc = g.restore("restore0-request", 0, DATE0, RESTORE, KEY)
    g.check("the level 0 restore exits 2 (in progress)", rc == 2)
    g.check("it requests retrieval", g.log_has("requesting retrieval"))
    rc = g.restore("restore0-repeat", 0, DATE0, RESTORE, KEY)
    g.check("a repeated restore exits 2", rc == 2)
    g.check("it requests no retrieval again", not g.log_has("requesting retrieval"))

    # The level 1 restore checks that its parent is on the target before requesting any
    # retrieval, so it is pointed at a copy of the level 0 snapshot received locally. Its state
    # is keyed by backup, not target, so finishing onto the real target later picks it up.
    send = subprocess.Popen(["btrfs", "send", "-q", SNAPS / SNAP0], stdout=subprocess.PIPE)
    btrfs("receive", SEED, stdin=send.stdout)
    if send.wait() != 0:
        raise subprocess.CalledProcessError(send.returncode, send.args)
    rc = g.restore("restore1-request", 1, DATE1, SEED, KEY)
    g.check("the level 1 restore exits 2 (in progress)", rc == 2)
    g.check("it requests retrieval", g.log_has("requesting retrieval"))
    btrfs("subvolume", "delete", SEED / SNAP0)

    reject_https = ["OUTPUT", "-p", "tcp", "--dport", "443", "-j", "REJECT"]
    run("iptables", "-I", *reject_https)
    try:
        rc = g.restore("restore0-offline", 0, DATE0, RESTORE, KEY)
    finally:
        run("iptables", "-D", *reject_https)
    g.check("a restore with the network cut exits 1", rc == 1)


def release_smoke_test(g):
    release = Guest(g.bucket, f"{g.prefix}release/", RELEASE_BIN, RELEASE_CFG)
    RELEASE_SNAPS.mkdir()
    btrfs("subvolume", "create", RELEASE_DATA)
    write_random(RELEASE_DATA / "payload.bin", 15 * MiB)
    btrfs("subvolume", "snapshot", "-r", RELEASE_DATA, RELEASE_SNAPS / SNAP0)
    write_config(
        RELEASE_CFG,
        bucket=release.bucket,
        prefix=release.prefix,
        public_key=load_state()["public_key"],
        base_dir=DRILL_DIR / "release-base",
        subvolume=RELEASE_DATA,
        snapshot_dir=RELEASE_SNAPS,
    )

    rc = release.bcb("release-check", "check")
    release.check("release bcb check passes", rc == 0)
    rc = release.bcb("release-backup0", "backup", "--level", 0)
    release.check("release level 0 backup succeeds", rc == 0)
    release.check_parts(0, "DEEP_ARCHIVE", 1)
    release.check_manifest(0)
    rc = release.bcb("release-list", "list")
    release.check("release bcb list succeeds", rc == 0)
    rc = release.bcb("release-verify", "verify")
    release.check("release bcb verify succeeds", rc == 0)


def mark_waiting(g):
    state = load_state()
    state["phase"] = "waiting"
    save_state(state)


def finish(g):
    state = load_state()
    # A completed restore removes its work dir, so running it again would start over; each
    # level's completion is recorded instead.
    for level, date in ((0, DATE0), (1, DATE1)):
        if level in state["done"]:
            continue
        rc = g.restore(f"finish{level}", level, date, RESTORE, KEY)
        if rc == 2:
            print(f"level {level}: retrieval still in progress")
            sys.exit(2)
        g.check(f"the level {level} restore succeeds", rc == 0)
        state["done"].append(level)
        save_state(state)

    for level, snap in ((0, SNAP0), (1, SNAP1)):
        diff = subprocess.run(["diff", "-r", SNAPS / snap, RESTORE / snap], check=False)
        g.check(f"level {level} restores byte-identical", diff.returncode == 0)
    reset_drill_dirs()


GUEST_STEPS = {
    step.__name__: step
    for step in (
        setup,
        check_permissions,
        check_no_tag,
        backup_level0,
        backup_level1,
        restore_denied,
        request_retrieval,
        release_smoke_test,
        mark_waiting,
        finish,
    )
}


def guest_main(step):
    # Credentials arrive over stdin so they never appear in a command line or a file in the VM.
    payload = json.load(sys.stdin)
    os.environ.update(payload["env"])
    os.environ.update(
        AWS_DEFAULT_REGION="us-east-1",
        RUST_LOG="info",
        NO_COLOR="1",
        # Larger than one multipart chunk, so each part is uploaded in several chunks.
        BCB_TEST_SPLIT_SIZE=str(80 * MiB),
    )
    GUEST_STEPS[step](Guest(payload["bucket"], payload["prefix"], BIN, CFG))


# Host: builds bcb, prepares the VM and runs the guest steps in order.
class Host:
    def __init__(self, vm, bucket, profile, no_tag_profile, restore_profile):
        self.vm = vm
        self.bucket = bucket
        self.profile = profile
        self.no_tag_profile = no_tag_profile
        self.restore_profile = restore_profile

    # Never run `multipass exec` with stdout sent to DEVNULL: it then busy-loops at full CPU.
    def vm_exec(self, *cmd, check=False, **kwargs):
        cmd = ["multipass", "exec", self.vm, "--", *map(str, cmd)]
        return subprocess.run(cmd, check=check, **kwargs)

    def vm_exists(self):
        info = subprocess.run(["multipass", "info", self.vm], check=False, capture_output=True)
        return info.returncode == 0

    def vm_state(self):
        """The state of the VM's drill run, or None if it has none."""
        if not self.vm_exists():
            return None
        cat = self.vm_exec("sudo", "cat", STATE_FILE, capture_output=True, text=True)
        return json.loads(cat.stdout) if cat.returncode == 0 else None

    def vm_upload(self, src, dest):
        # Removed first: transferring onto a binary that is still running fails.
        self.vm_exec("sudo", "rm", "-f", dest, check=True)
        run("multipass", "transfer", src, f"{self.vm}:{dest}")
        self.vm_exec("chmod", "+x", dest, check=True)

    def vm_target(self):
        """The Rust target triple for the VM, which matches the host's architecture rather
        than a fixed one, so the drill works on x86_64 and Apple silicon hosts."""
        arch = self.vm_exec("uname", "-m", check=True, capture_output=True, text=True)
        arch = arch.stdout.strip()
        if arch not in ("aarch64", "x86_64"):
            sys.exit(f"unsupported VM architecture: {arch}")
        return f"{arch}-unknown-linux-gnu"

    def credentials(self, profile):
        out = output(
            "aws",
            "configure",
            "export-credentials",
            "--profile",
            profile,
            "--format",
            "process",
        )
        creds = json.loads(out)
        env = {
            "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
        }
        if "SessionToken" in creds:
            env["AWS_SESSION_TOKEN"] = creds["SessionToken"]
        return env

    def vm_run(self, profile, step, prefix):
        """Runs one guest step as root in the VM with a profile's credentials; returns its exit
        code."""
        payload = {
            "env": self.credentials(profile),
            "bucket": self.bucket,
            "prefix": prefix,
        }
        guest = self.vm_exec(
            "sudo",
            "python3",
            "-u",
            VM_SCRIPT,
            "guest",
            step,
            input=json.dumps(payload),
            text=True,
        )
        return guest.returncode

    def check_host(self):
        for cmd in ("aws", "multipass", "cargo-zigbuild"):
            if not shutil.which(cmd):
                sys.exit(f"{cmd} is required on the host")
        # Checked before the build, which takes minutes, rather than at first use after it.
        for profile in (self.profile, self.no_tag_profile, self.restore_profile):
            export = subprocess.run(
                ["aws", "configure", "export-credentials", "--profile", profile],
                check=False,
                stdout=subprocess.DEVNULL,
            )
            if export.returncode != 0:
                sys.exit(f"AWS profile '{profile}' is not usable; see docs/aws-drill.md")
        # The drill's lifecycle rules expire what it writes; they must never share a bucket
        # with backups someone depends on.
        top = s3api(
            "list-objects-v2",
            self.bucket,
            "--delimiter",
            "/",
            "--profile",
            self.profile,
        )
        foreign = [p["Prefix"] for p in top.get("CommonPrefixes", [])]
        foreign += [o["Key"] for o in top.get("Contents", [])]
        foreign = [name for name in foreign if name != "aws-drill/"]
        if foreign:
            sys.exit(
                f"s3://{self.bucket} holds data that is not from the drill; "
                "use a dedicated bucket:\n" + "\n".join(foreign[:5])
            )

    def prepare_vm(self):
        self.check_host()
        if not self.vm_exists():
            print(f"==> Launching VM {self.vm}...")
            run(
                "multipass",
                "launch",
                "--name",
                self.vm,
                "--disk",
                "20G",
                "--memory",
                "2G",
                "--cpus",
                "2",
            )
        run(
            "multipass",
            "transfer",
            SCRIPT_DIR / "setup-vm.sh",
            f"{self.vm}:setup-vm.sh",
        )
        setup_vm = self.vm_exec(
            "sudo",
            "bash",
            "setup-vm.sh",
            "--no-s3",
            check=True,
            capture_output=True,
            text=True,
        )
        print(setup_vm.stdout.splitlines()[-1])

        target = self.vm_target()
        run("rustup", "target", "add", target, stdout=subprocess.DEVNULL)
        # The dev profile keeps the BCB_TEST_* hooks the drill drives; the release build is
        # what production runs.
        print("==> Building bcb (dev and release)...")
        run("cargo", "zigbuild", "--bin", "bcb", "--target", target, cwd=REPO_DIR)
        run(
            "cargo",
            "zigbuild",
            "--release",
            "--bin",
            "bcb",
            "--target",
            target,
            cwd=REPO_DIR,
        )
        self.vm_upload(REPO_DIR / "target" / target / "debug" / "bcb", BIN)
        self.vm_upload(REPO_DIR / "target" / target / "release" / "bcb", RELEASE_BIN)
        self.vm_upload(Path(__file__).resolve(), VM_SCRIPT)

    def start_run(self):
        now = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        commit = output("git", "-C", SCRIPT_DIR, "rev-parse", "--short", "HEAD").strip()
        prefix = f"aws-drill/run-{now}-{commit}/"
        print(f"==> Starting a new drill run under s3://{self.bucket}/{prefix}")
        self.prepare_vm()

        steps = [
            ("Setting up the test subvolume and config", self.profile, "setup"),
            ("Checking the production policy", self.profile, "check_permissions"),
            ("Checking a policy without tagging", self.no_tag_profile, "check_no_tag"),
            (
                "Level 0 backup, killed mid-upload and resumed",
                self.profile,
                "backup_level0",
            ),
            ("Level 1 backup", self.profile, "backup_level1"),
            ("Restoring with the backup policy", self.profile, "restore_denied"),
            ("Requesting retrieval", self.restore_profile, "request_retrieval"),
            ("Release build smoke test", self.profile, "release_smoke_test"),
            ("Marking the run as waiting on retrieval", self.profile, "mark_waiting"),
        ]
        for title, profile, step in steps:
            print(f"==> {title}")
            rc = self.vm_run(profile, step, prefix)
            if rc != 0:
                sys.exit(rc)

        print("==> Retrieval requested. Re-run this in about 12 hours to finish the restores.")
        sys.exit(2)

    def finish_run(self, prefix):
        print("==> Finishing the waiting drill run")
        # Uploaded again so a fix made to this script while waiting takes effect.
        self.vm_upload(Path(__file__).resolve(), VM_SCRIPT)
        rc = self.vm_run(self.restore_profile, "finish", prefix)
        if rc == 0:
            print("==> AWS drill passed")
        elif rc == 2:
            print("==> Still waiting on retrieval; re-run later.")
        sys.exit(rc)


def host_main(args):
    # Exits 1 rather than argparse's 2, which would read as "waiting on retrieval".
    if args not in ([], ["--new"]):
        sys.exit(f"usage: {sys.argv[0]} [--new]")
    bucket = os.environ.get("BCB_DRILL_BUCKET")
    if not bucket:
        sys.exit("set BCB_DRILL_BUCKET to the drill bucket (see docs/aws-drill.md)")
    host = Host(
        vm=os.environ.get("VM", "bcb-aws"),
        bucket=bucket,
        profile=os.environ.get("BCB_DRILL_PROFILE", "bcb-drill"),
        no_tag_profile=os.environ.get("BCB_DRILL_NO_TAG_PROFILE", "bcb-drill-no-tag"),
        restore_profile=os.environ.get("BCB_DRILL_RESTORE_PROFILE", "bcb-drill-restore"),
    )

    state = None if args == ["--new"] else host.vm_state()
    if state and state.get("phase") == "waiting":
        host.finish_run(state["prefix"])
    else:
        host.start_run()


if __name__ == "__main__":
    if sys.argv[1:2] == ["guest"]:
        guest_main(sys.argv[2])
    else:
        host_main(sys.argv[1:])
