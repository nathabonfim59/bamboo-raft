"""Fake findmnt for the data-mount control (tests/data-mount.yml).

Usage: fake-findmnt.py <table.json> --noheadings --output OPTIONS --target <path>

Prints the options of the longest table prefix of <path>, like findmnt's
--target resolution of the containing mount. Never touches real mounts.
"""

import json
import sys


def main() -> int:
    table = json.load(open(sys.argv[1], encoding="utf-8"))
    args = sys.argv[2:]
    if args[:4] != ["--noheadings", "--output", "OPTIONS", "--target"] or len(args) != 5:
        print(f"unexpected arguments {args}", file=sys.stderr)
        return 2
    target = args[4]
    best = max((m for m in table if target == m or target.startswith(m.rstrip("/") + "/")), key=len, default=None)
    if best is None:
        return 1
    print(table[best])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
