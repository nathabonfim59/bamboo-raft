#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Move a bamboo-raft node VM from VirtualBox to Hyper-V with nested
  virtualization, so the Linux guest gets VT-x and /dev/kvm.

.DESCRIPTION
  See docs/windows-hyperv-node.md. Steps:
    1. Install Hyper-V if missing (then asks for a reboot; run again after).
    2. Find the disk attached to the VirtualBox VM (must be powered off).
    3. Clone it to VHD (VirtualBox cannot create VHDX), convert it to a dynamic
       VHDX with Convert-VHD.
    4. Create a Generation 1 Hyper-V VM on it (the disk is MBR/BIOS).
    5. Static memory, all CPUs, ExposeVirtualizationExtensions, no checkpoints.
    6. Start it.
  Safe to re-run: an existing VHDX or VM is reused and only its settings change.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\bbraft-hyperv.ps1 -AutoStart -MemoryGB 16
#>
param(
  [string]$VBoxVM   = "bbraft",          # VirtualBox VM name
  [string]$HvVM     = "bbraft",          # Hyper-V VM name
  [string]$VMDir    = "C:\VMs",          # where the .vhdx goes
  [int]   $MemoryGB = 16,                # static memory; must be free when the VM starts
  [int]   $CPUs     = 0,                 # 0 = all host logical processors
  [string]$Switch   = "Default Switch",  # NAT switch Hyper-V creates
  [switch]$AutoStart                     # start the VM when Windows boots
)
$ErrorActionPreference = "Stop"
function Step($m) { Write-Host "`n== $m" -ForegroundColor Cyan }

# --- 1. Hyper-V ---------------------------------------------------------------
Step "Hyper-V feature"
$hv = Get-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V-All
if ($hv.State -ne "Enabled") {
  Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V-All -All -NoRestart | Out-Null
  Write-Host "Hyper-V installed. Reboot Windows (shutdown /r /t 0), then run this script again." -ForegroundColor Yellow
  exit 0
}
Import-Module Hyper-V
Write-Host "Hyper-V is enabled."

# --- 2. Find the VirtualBox disk ----------------------------------------------
$vhdx = Join-Path $VMDir "$HvVM.vhdx"
if (Test-Path $vhdx) {
  Step "Disk"
  Write-Host "$vhdx already exists, skipping the conversion."
} else {
  Step "VirtualBox disk of '$VBoxVM'"
  $vbm = Join-Path $env:ProgramFiles "Oracle\VirtualBox\VBoxManage.exe"
  if (-not (Test-Path $vbm)) { throw "VBoxManage not found at $vbm" }

  $info = & $vbm showvminfo $VBoxVM --machinereadable
  if ($LASTEXITCODE -ne 0) { throw "VirtualBox VM '$VBoxVM' not found (VBoxManage list vms)" }

  # A saved state holds unwritten guest data in memory: copying the disk then
  # gives an unclean filesystem. Require a real power-off.
  $state = ($info | Select-String '^VMState="(.+)"$').Matches[0].Groups[1].Value
  if ($state -ne "poweroff") {
    throw "VirtualBox VM is '$state'. Start it, shut it down from inside (sudo poweroff), and run again. Do not use 'discardstate'."
  }

  $disk = $info |
    Select-String '^"[^"]+-\d+-\d+"="(.+\.(vdi|vmdk|vhd))"$' |
    ForEach-Object { $_.Matches[0].Groups[1].Value } |
    Select-Object -First 1
  if (-not $disk) {
    $disk = (Get-ChildItem (Join-Path $HOME "VirtualBox VMs\$VBoxVM") -Recurse -Include *.vdi,*.vmdk,*.vhd |
             Sort-Object Length -Descending | Select-Object -First 1).FullName
  }
  if (-not $disk) { throw "No disk found for '$VBoxVM'" }
  if ($disk -match '\\Snapshots\\') {
    throw "Attached disk is a snapshot ($disk). Delete the VM's snapshots in VirtualBox first."
  }
  Write-Host "Disk: $disk"

  # --- 3. Convert ---------------------------------------------------------------
  # VirtualBox reads VHDX but cannot create it (VERR_INVALID_PARAMETER), so it
  # writes VHD and Hyper-V converts.
  New-Item -ItemType Directory -Force $VMDir | Out-Null
  $vhd = [IO.Path]::ChangeExtension($vhdx, ".vhd")
  if (-not (Test-Path $vhd)) {
    Step "Clone to $vhd (dynamic VHD)"
    & $vbm clonemedium disk $disk $vhd --format VHD --variant Standard
    if ($LASTEXITCODE -ne 0) { Remove-Item $vhd -ErrorAction SilentlyContinue; throw "clonemedium failed" }
    & $vbm closemedium disk $vhd | Out-Null
  }
  Step "Convert $vhd -> $vhdx (dynamic VHDX)"
  Convert-VHD -Path $vhd -DestinationPath $vhdx -VHDType Dynamic
  Remove-Item $vhd
}

# --- 4. Create the Hyper-V VM -------------------------------------------------
Step "Hyper-V VM '$HvVM'"
$cs      = Get-CimInstance Win32_ComputerSystem
$hostLP  = $cs.NumberOfLogicalProcessors
$freeGB  = [math]::Floor((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1MB)
if ($CPUs -le 0 -or $CPUs -gt $hostLP) { $CPUs = $hostLP }
if ($MemoryGB -ge $freeGB) {
  Write-Host "Warning: $MemoryGB GB requested, $freeGB GB free now. Start-VM may fail with 0x8007000E; close apps or pass a smaller -MemoryGB." -ForegroundColor Yellow
}
if (-not (Get-VMSwitch -Name $Switch -ErrorAction SilentlyContinue)) {
  throw "Hyper-V switch '$Switch' not found (Get-VMSwitch)"
}

$vm = Get-VM -Name $HvVM -ErrorAction SilentlyContinue
if ($vm) {
  Write-Host "VM exists, updating its settings."
  if ($vm.State -ne "Off") { Stop-VM -Name $HvVM -Force }
} else {
  # Generation 1: the VirtualBox disk is MBR/BIOS; Generation 2 needs GPT/UEFI.
  New-VM -Name $HvVM -Generation 1 -MemoryStartupBytes ($MemoryGB * 1GB) `
         -VHDPath $vhdx -SwitchName $Switch -Path $VMDir | Out-Null
}

# --- 5. Nested virtualization and fixed resources -----------------------------
Step "Nested virtualization, $CPUs CPUs, $MemoryGB GB static"
# Nested virtualization requires static memory and no checkpoints.
Set-VMMemory    -VMName $HvVM -DynamicMemoryEnabled $false -StartupBytes ($MemoryGB * 1GB)
Set-VMProcessor -VMName $HvVM -Count $CPUs -ExposeVirtualizationExtensions $true
Set-VM -Name $HvVM -AutomaticCheckpointsEnabled $false -CheckpointType Disabled `
       -AutomaticStopAction ShutDown
if ($AutoStart) { Set-VM -Name $HvVM -AutomaticStartAction Start -AutomaticStartDelay 30 }
else            { Set-VM -Name $HvVM -AutomaticStartAction Nothing }

# --- 6. Start -----------------------------------------------------------------
Step "Start"
Start-VM -Name $HvVM
Get-VMProcessor -VMName $HvVM | Select-Object VMName, Count, ExposeVirtualizationExtensions | Format-Table
Get-VM -Name $HvVM | Select-Object Name, State, MemoryStartup, Uptime | Format-Table

Write-Host @"

Done. Next:
  - Do not start the VirtualBox '$VBoxVM' as well: both share one Tailscale identity.
  - Console: vmconnect localhost $HvVM
  - In the guest: ls -l /dev/kvm   (then continue with infra/README.md)
  - Delete the VirtualBox VM once this one works.
"@ -ForegroundColor Green
