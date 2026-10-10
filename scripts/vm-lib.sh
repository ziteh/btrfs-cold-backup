# Shared by run-vm-tests.sh and power-loss-drill.sh; source it, don't run it.
#
# `multipass exec` has two quirks every caller has to avoid:
#  - with stdout redirected to /dev/null it busy-loops at full CPU, so output is piped to
#    `cat` instead;
#  - a background child of the session dies with it, so anything that must outlive the call
#    has to be started as a systemd unit.

VM=${VM:-bcb-dev}

vm_exec() {
    multipass exec "$VM" -- sudo bash -c "$1" | cat
    return "${PIPESTATUS[0]}"
}

# Copies setup-vm.sh into the VM and runs it with the given arguments.
vm_setup() {
    multipass transfer "$SCRIPT_DIR/setup-vm.sh" "$VM:setup-vm.sh"
    multipass exec "$VM" -- sudo bash setup-vm.sh "$@" | cat
    return "${PIPESTATUS[0]}"
}

# The Rust target triple for the VM, which matches the host's architecture rather than a
# fixed one, so the same scripts work on x86_64 and Apple silicon hosts.
vm_target() {
    local arch
    arch=$(multipass exec "$VM" -- uname -m | cat)
    case "$arch" in
        aarch64|x86_64) echo "$arch-unknown-linux-gnu" ;;
        *) echo "unsupported VM architecture: $arch" >&2; return 1 ;;
    esac
}

# Copies a host binary into the VM as /home/ubuntu/<name>.
vm_upload() {
    local src=$1 name=$2
    # Removed first: transferring onto a binary that is still running fails.
    multipass exec "$VM" -- sudo rm -f "/home/ubuntu/$name"
    multipass transfer "$src" "$VM:/home/ubuntu/$name"
    multipass exec "$VM" -- chmod +x "/home/ubuntu/$name"
}

wait_for_vm() {
    for _ in $(seq 1 60); do
        multipass exec "$VM" -- true 2>/dev/null && return
        sleep 3
    done
    echo "VM did not come back" >&2
    return 1
}
