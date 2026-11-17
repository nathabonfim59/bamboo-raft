"""Print a deterministic snapshot of a directory tree, never following links.

Usage: python3 tree_snapshot.py <root>

One line per entry (the root included): relative path, type, inode, uid,
gid, mode, link count, size and a sha256 of regular-file bytes or the link
target. The legacy data-dir test compares snapshots before and after a
refused role run: identical output proves no ownership, inode or byte changed.
"""

from __future__ import annotations

import hashlib
import os
import stat
import sys


def describe(path: str, rel: str) -> str:
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode):
        kind, digest = "link", hashlib.sha256(os.readlink(path).encode()).hexdigest()
    elif stat.S_ISREG(st.st_mode):
        with open(path, "rb") as f:
            kind, digest = "file", hashlib.sha256(f.read()).hexdigest()
    elif stat.S_ISDIR(st.st_mode):
        kind, digest = "dir", "-"
    else:
        kind, digest = "other", "-"
    return f"{rel} {kind} {st.st_ino} {st.st_uid} {st.st_gid} {stat.S_IMODE(st.st_mode):04o} {st.st_nlink} {st.st_size} {digest}"


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    root = sys.argv[1]
    lines = [describe(root, ".")]
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(dirnames + filenames):
            full = os.path.join(dirpath, name)
            lines.append(describe(full, os.path.relpath(full, root)))
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
