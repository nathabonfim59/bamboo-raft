"""Prove the rendered ExecStart is accepted by the real bbraft-node CLI.

Usage: python3 check_unit_flags.py <bbraft-node-binary> <rendered-unit-file>

The unit is parsed the way systemd joins continuation lines. Every argument
before the trailing -h is handed to the real binary, so the Go flag set parses
each flag name and value (string, duration, uint, float). The binary exits with
"flag: help requested" and status 1 when -h is reached; an unknown flag or an
invalid value exits earlier with status 2. A second, flag-only run checks that
every rendered flag is listed by the binary's own usage output.

No flag values are secrets: the unit contains only file paths and settings.
"""

from __future__ import annotations

import re
import shlex
import subprocess
import sys


def exec_start_argv(unit_path: str) -> list[str]:
    lines = open(unit_path, encoding="utf-8").read().splitlines()
    for index, line in enumerate(lines):
        if not line.startswith("ExecStart="):
            continue
        command = line[len("ExecStart=") :]
        while command.endswith("\\"):
            index += 1
            if index >= len(lines):
                raise SystemExit("ExecStart ends with a dangling continuation")
            command = command[:-1] + lines[index]
        return shlex.split(command)
    raise SystemExit(f"{unit_path}: no ExecStart= line")


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    binary, unit_path = sys.argv[1], sys.argv[2]
    argv = exec_start_argv(unit_path)
    if not argv:
        raise SystemExit("empty ExecStart")

    # Every rendered flag takes a value (the unit has no boolean flags), so
    # the arguments are name/value pairs. A value may itself start with '-'
    # (-node.cpu-features -vmx-apicv-vid); Go's flag package takes it as the
    # value, so only even positions are flag names.
    if len(argv[1:]) % 2 != 0:
        raise SystemExit(f"ExecStart has an unpaired flag: {argv[1:]}")
    flags = argv[1::2]
    if not all(flag.startswith("-") for flag in flags):
        raise SystemExit(f"ExecStart has a value where a flag belongs: {flags}")
    if "-h" in flags or "-help" in flags or "--help" in flags:
        raise SystemExit("the unit unexpectedly contains a help flag")

    usage = subprocess.run(
        [binary, "-h"], capture_output=True, text=True, check=False
    )
    usage_text = usage.stdout + usage.stderr
    listed = set(re.findall(r"^\s+-([A-Za-z0-9._-]+)", usage_text, re.MULTILINE))
    missing = sorted({flag[1:] for flag in flags} - listed)
    if missing:
        raise SystemExit(f"flags missing from {binary} -h: {', '.join(missing)}")

    parsed = subprocess.run(
        [binary, *argv[1:], "-h"], capture_output=True, text=True, check=False
    )
    output = parsed.stdout + parsed.stderr
    if parsed.returncode == 2 or "flag provided but not defined" in output or "invalid value" in output:
        sys.stderr.write(output)
        raise SystemExit(f"{binary} rejected the rendered ExecStart")
    if parsed.returncode != 1 or "help requested" not in output:
        sys.stderr.write(output)
        raise SystemExit(f"{binary} did not stop at -h as expected (status {parsed.returncode})")

    print(f"ok: {len(flags)} rendered flags and values parsed by {binary} ({unit_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
