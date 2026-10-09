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

wait_for_vm() {
    for _ in $(seq 1 60); do
        multipass exec "$VM" -- true 2>/dev/null && return
        sleep 3
    done
    echo "VM did not come back" >&2
    return 1
}
