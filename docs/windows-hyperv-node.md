# A bamboo-raft node on Windows (Hyper-V, nested KVM)

How to run a bamboo-raft node inside a Linux VM on a Windows machine, for
example a dev box for the team. `bbraft-node` needs `/dev/kvm`, so the VM
needs nested virtualization: the Windows hypervisor exposes VT-x to the
Linux guest, and KVM runs inside it.

The path is: **install Debian in VirtualBox** (easy installer, console,
copy-paste), **prepare it**, then **convert the disk to Hyper-V**, which is
where it runs. After that the node is provisioned like any other host with
`infra/ansible` (see [infra/README.md](../infra/README.md)).

## Why Hyper-V and not VirtualBox

On Windows 11, virtualization-based security (VBS) is usually on: Memory
Integrity (HVCI), System Guard Secure Launch, Credential Guard. Any of them
starts the Windows hypervisor at boot, and it keeps VT-x for itself.
VirtualBox then runs on top of it through the Windows Hypervisor Platform
("NEM", the green turtle icon) and **cannot give its VMs VT-x**, whatever
`--nested-hw-virt` says. Its log shows:

```
HM: HMR3Init: Attempting fall back to NEM: VT-x is not available
NEM: WHvCapabilityCodeHypervisorPresent is TRUE, so this might work...
```

Turning VBS off works only when nothing enforces it; on a machine joined to
Microsoft Entra ID / Intune, a device policy turns it back on. Hyper-V's own
nested virtualization works **with** VBS on, so the machine keeps its
security baseline.

## Requirements

| | |
|---|---|
| Windows | 11 Pro, Enterprise or Education (Hyper-V is not in Home) |
| CPU | Intel VT-x with EPT, or AMD-V (AMD nested needs Windows 11 / Server 2022) |
| Firmware | virtualization on in BIOS/UEFI (Task Manager → CPU → "Virtualization: Enabled") |
| Memory | the VM's memory is **static** and must be **free** when it starts |
| Disk | 250 GB or more for the VM's disk (dynamic, grows as used) |
| Guest | **Debian 13 (trixie)**: the node role needs systemd ≥ 254 (`DelegateSubgroup=`); Debian 12 has 252 |

## 1. Install Debian 13 in VirtualBox

Create the VM with:

- **Type** Linux / Debian 64-bit, **firmware BIOS** (the default; EFI off).
  The disk then gets an MBR partition table, which a Hyper-V Generation 1 VM
  boots as is.
- **Memory** what the Hyper-V VM will get later (for example 16 GB).
  **CPUs** as many as you want the node to have.
- **Disk** a dynamic VDI of 250 GB or more. VDI, not VMDK: VirtualBox resizes
  and converts VDI best. No snapshots.
- **System → Processor → PAE/NX** on (64-bit guests need NX).

Install from the Debian 13 netinst ISO. At **Partition disks** choose
*Manual* and lay it out like a bare-metal node (root separate from VM data):

| Partition | Size | Mount | Notes |
|---|---|---|---|
| 1 primary, ext4, bootable | 80 GB | `/` | OS, packages, Postgres |
| 2 primary, swap | 4 GB | – | |
| 3 primary, ext4 | rest | `/var/lib/bbraft` | node data and guest images |

At **Software selection** pick *SSH server* and *standard system utilities*.
A desktop is optional; step 2 makes the VM boot without it.

### Already installed with the default layout?

If the VM has the installer's default layout (`/` then an extended partition
with swap at the end) and you grew the disk afterwards
(`VBoxManage modifymedium disk <file>.vdi --resize <MB>`, VM powered off),
repartition from inside the running VM as root. Save the old table first:

```sh
sfdisk -d /dev/sda > /root/sda-before.sfdisk; cp /etc/fstab /root/fstab.before
swapoff -a
sfdisk --no-reread --delete /dev/sda 5            # logical swap
sfdisk --no-reread --delete /dev/sda 2            # extended partition
echo ',80G' | sfdisk --no-reread -N 1 /dev/sda    # grow / (same start sector)
printf ',4G,S\n,,L\n' | sfdisk --no-reread --append /dev/sda   # swap, data
partx -u /dev/sda; partx -a /dev/sda; udevadm settle
mkswap -L bbraft-swap /dev/sda2
mkfs.ext4 -L bbraft-data /dev/sda3
# fstab: replace the old swap UUID with sda2's, add sda3 on /var/lib/bbraft
blkid /dev/sda2 /dev/sda3; editor /etc/fstab
echo "RESUME=UUID=$(blkid -s UUID -o value /dev/sda2)" > /etc/initramfs-tools/conf.d/resume
update-initramfs -u
mkdir -p /var/lib/bbraft && mount /var/lib/bbraft && swapon -a
reboot
# after the reboot the kernel sees the new sda1 size:
resize2fs /dev/sda1
```

## 2. Prepare the guest

As root in the VM:

```sh
# Ansible becomes root without a password
echo 'debian ALL=(ALL:ALL) NOPASSWD: ALL' > /etc/sudoers.d/90-debian-nopasswd
chmod 440 /etc/sudoers.d/90-debian-nopasswd && visudo -c

# Boot to text mode (SSH and network still start); the GUI stays available
systemctl set-default multi-user.target

# Tailscale, tagged for the node (infra/README.md, "Before the first run")
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --auth-key=file:/path/to/authkey --advertise-tags=tag:bbraft

# VirtualBox Guest Additions are useless on Hyper-V; remove them now
/opt/VBoxGuestAdditions-*/uninstall.sh 2>/dev/null || true
```

To keep a GUI choice in the boot menu instead of `set-default`, add a GRUB
entry before Debian's own (`/etc/grub.d/09_text`, then `update-grub`):

```sh
#!/bin/sh
cat <<'EOF'
menuentry 'Debian GNU/Linux (text mode: ssh + network, no GUI)' --class debian {
	insmod part_msdos
	insmod ext2
	search --no-floppy --fs-uuid --set=root <ROOT-UUID>
	linux /vmlinuz root=UUID=<ROOT-UUID> ro quiet systemd.unit=multi-user.target
	initrd /initrd.img
}
EOF
```

With `GRUB_DEFAULT=0` it becomes the default and the normal (graphical)
entry is second.

Then **shut the VM down from inside** (`sudo poweroff`). Not "save state":
a saved VM has unwritten data in its memory image, and Windows saves running
VirtualBox VMs when it reboots.

## 3. Convert to Hyper-V

[`deploy/windows/bbraft-hyperv.ps1`](../deploy/windows/bbraft-hyperv.ps1) does
it in one go. From an **elevated** PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\bbraft-hyperv.ps1 -AutoStart -MemoryGB 16
```

It:

1. installs Hyper-V if missing and asks for a reboot (run it again after);
2. finds the disk attached to the VirtualBox VM and refuses a VM that is not
   powered off or a disk that is a snapshot;
3. clones it to a dynamic VHD and converts that to a dynamic VHDX
   (VirtualBox cannot create VHDX);
4. creates a **Generation 1** VM on the `Default Switch` (NAT);
5. sets **static memory**, all logical CPUs, `ExposeVirtualizationExtensions`,
   no checkpoints, and with `-AutoStart` starts the VM with Windows;
6. starts it.

Parameters: `-VBoxVM`, `-HvVM` (both `bbraft`), `-VMDir` (`C:\VMs`),
`-MemoryGB` (16), `-CPUs` (0 = all), `-Switch`, `-AutoStart`. Re-running is
safe: an existing VHDX or VM is reused.

Do not run the VirtualBox VM and the Hyper-V VM at the same time: they share
one Tailscale identity. Delete the VirtualBox VM once the Hyper-V one works.

## 4. Point GRUB at the Hyper-V disk

The VM boots fine after the conversion (GRUB's code is in the disk's boot
sector), but `grub-pc` remembers **where to reinstall itself** by the disk's
id, and that id was VirtualBox's (`/dev/disk/by-id/ata-VBOX_HARDDISK_…`).
The Hyper-V disk has a different one, so the next `grub-pc` upgrade fails
with "`… does not exist, so cannot grub-install to it!`" and leaves dpkg
half-configured (a `bookworm` → `trixie` upgrade hits this every time). Fix
it on the first boot under Hyper-V, as root:

```sh
debconf-show grub-pc | grep install_devices:    # still the ata-VBOX_HARDDISK_… id
ls -l /dev/disk/by-id/ | grep -v part           # the Hyper-V disk: wwn-0x… -> ../../sda
DISK=$(ls /dev/disk/by-id/wwn-* | grep -v -- -part | head -1)
readlink -f "$DISK"                             # must print /dev/sda
echo "grub-pc grub-pc/install_devices multiselect $DISK" | debconf-set-selections
dpkg-reconfigure -f noninteractive grub-pc      # reinstalls GRUB there
debconf-show grub-pc | grep install_devices:    # now the wwn-0x… id
```

Look for `Installation finished. No error reported.` in the output. If an
upgrade already failed on it, run the `debconf-set-selections` line, then
`dpkg --configure -a` and `apt-get -f install`, and **reboot only after**
`grub-install success for /dev/sda`.

## 5. Check KVM in the guest

```sh
lscpu | grep -E 'Hypervisor vendor|Virtualization:'   # Microsoft, VT-x
ls -l /dev/kvm /dev/vhost-vsock
lsmod | grep kvm_intel                                 # kvm_amd on AMD
```

The network interface is now `eth0` (Hyper-V's synthetic NIC) with an
address from the Default Switch; NetworkManager configures it and the
tailnet address does not change.

Then provision the node: [infra/README.md](../infra/README.md). Choose how
the API appears on the tailnet before the first run: HTTPS needs HTTPS
certificates in the tailnet and puts `<machine>.<tailnet>.ts.net` in the
public Certificate Transparency logs; `tailscale_serve_scheme: http` needs
neither and is still encrypted by WireGuard (see
[API access: HTTPS or HTTP](../infra/README.md#api-access-https-or-http)).
A dev box on a company tailnet is usually better off with `http`.

**How many sandboxes fit** depends mostly on the VM's memory: with 16–20 GiB
static memory expect about 4–7 `small` or 2–3 `default` sandboxes running at
once (disks are thin and rarely the limit). See
[docs/node.md, "Sizing a node"](node.md#sizing-a-node) before raising
`bbraft_node_memory_overcommit` above 1 on a VM this size. Hyper-V
nested VMs cannot take checkpoints, so back up by copying the VHDX with the
VM off (`Stop-VM bbraft; Copy-Item C:\VMs\bbraft.vhdx <backup>`).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| VirtualBox guest: no `vmx` in `/proc/cpuinfo`, no `/dev/kvm`; `VBox.log` has `NEM:` lines | the Windows hypervisor is running (VBS) | use Hyper-V (this guide) |
| `bcdedit /set hypervisorlaunchtype off` changes nothing | VBS starts the hypervisor anyway | check `Get-CimInstance -Namespace root\Microsoft\Windows\DeviceGuard -ClassName Win32_DeviceGuard`: `VirtualizationBasedSecurityStatus` 2 = running |
| VBS turns itself back on | Intune / Group Policy (`HKLM\SOFTWARE\Policies\Microsoft\Windows\DeviceGuard`) | change the policy, or use Hyper-V and leave VBS on |
| In PowerShell `bcdedit /copy {current}` says "parameter incorrect" | PowerShell reads `{...}` as a script block | quote it: `'{current}'` |
| Script: "VirtualBox VM is 'saved'" | Windows saved the VM at reboot | start it, `sudo poweroff` inside, run again |
| `VD: backend 'VHDX' cannot create base images` | VirtualBox cannot write VHDX | the script clones to VHD and uses `Convert-VHD` |
| `Start-VM`: insufficient memory (`0x8007000E`) | static memory must be free at start | smaller `-MemoryGB`, or close apps; `Set-VMMemory -VMName bbraft -StartupBytes 12GB` |
| Hyper-V VM does not boot the disk | Generation 2 VM on an MBR disk | Generation 1 (the script's default) |
| Changing CPU or memory settings is refused | the VM is running | `Stop-VM bbraft`, change, `Start-VM bbraft` |
| Playbook hangs at "Publish the service over tailnet HTTPS" (older roles) or fails at "The tailnet issues HTTPS certificates" | HTTPS certificates are off in the tailnet | enable them (admin console, DNS), or `tailscale_serve_scheme: http` |
| Installing `systemd-resolved` breaks DNS (`Failed to activate service 'org.freedesktop.resolve1': timed out`) | dbus has not loaded resolved's new policy | `systemctl reload dbus; systemctl restart systemd-resolved NetworkManager tailscaled` |
| Node role: "bbraft-node needs systemd 254 or newer" | Debian 12 | upgrade to Debian 13 (`bookworm` → `trixie` in the apt sources, `apt full-upgrade`) |
| After the conversion, an upgrade fails at `grub-pc`: "`/dev/disk/by-id/ata-VBOX_HARDDISK_…` does not exist" | grub-pc remembers the VirtualBox disk id | `echo "grub-pc grub-pc/install_devices multiselect /dev/disk/by-id/wwn-…" \| debconf-set-selections` (the Hyper-V disk's id, `ls -l /dev/disk/by-id/`), then `dpkg --configure -a`; reboot only after "grub-install success" |
