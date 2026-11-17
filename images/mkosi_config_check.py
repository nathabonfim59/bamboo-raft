#!/usr/bin/env python3
"""Check mkosi's parsed configuration before a build.

Usage:
    mkosi_config_check.py --mkosi <pinned>/bin/mkosi [options] -- <exact mkosi args>

Options:
    --expect-snapshot VALUE|none   Require every parsed image's resolved
                                   Config.snapshot (none means None).
    --expect-mirror VALUE|none     Require every parsed image's resolved
                                   Config.mirror (none means None).

This imports the pinned mkosi source tree (the one behind the given executable)
and calls mkosi's own parse_config() on the exact argv the real build will
use, then checks the resolved Config objects (not the argv text) and calls
check_workspace_directory() on each image. Without expectations it behaves
exactly as before. It does not build, download packages or touch QEMU.
"""

import sys
from pathlib import Path


def usage() -> int:
	print(
		"usage: mkosi_config_check.py --mkosi <bin/mkosi> "
		"[--expect-snapshot VALUE|none] [--expect-mirror VALUE|none] -- <mkosi args>",
		file=sys.stderr,
	)
	return 2


def main() -> int:
	args = sys.argv[1:]
	mkosi_bin = None
	expect_snapshot = None
	expect_mirror = None

	while args:
		flag = args.pop(0)
		if flag == "--":
			break
		if flag == "--mkosi" and args:
			mkosi_bin = Path(args.pop(0)).resolve()
		elif flag == "--expect-snapshot" and args:
			expect_snapshot = args.pop(0)
		elif flag == "--expect-mirror" and args:
			expect_mirror = args.pop(0)
		else:
			return usage()

	if mkosi_bin is None or not args:
		return usage()

	mkosi_argv = args
	source = mkosi_bin.parent.parent
	if not (source / "mkosi" / "__init__.py").is_file():
		print(f"mkosi_config_check: {source} is not a mkosi source tree", file=sys.stderr)
		return 2
	sys.path.insert(0, str(source))

	import mkosi.resources  # noqa: E402
	from mkosi import check_workspace_directory  # noqa: E402
	from mkosi.config import parse_config  # noqa: E402

	resources = Path(mkosi.resources.__file__).parent
	_, _, images = parse_config(mkosi_argv, resources=resources)
	if not images:
		print("mkosi_config_check: no images parsed", file=sys.stderr)
		return 2

	for image in images:
		# The actual function mkosi calls before building.
		check_workspace_directory(image)
		workspace = image.workspace_dir_or_default()
		sources = ", ".join(str(tree.source) for tree in image.build_sources) or "none"
		print(
			f"mkosi_config_check: ok image={image.image} workspace={workspace} sources={sources} "
			f"snapshot={image.snapshot or 'none'} mirror={image.mirror or 'none'}"
		)

		if expect_snapshot is not None:
			got = image.snapshot
			if (expect_snapshot == "none" and got is not None) or (
				expect_snapshot != "none" and got != expect_snapshot
			):
				print(
					f"mkosi_config_check: image {image.image} resolved snapshot "
					f"{got or 'none'}, want {expect_snapshot}",
					file=sys.stderr,
				)
				return 1
		if expect_mirror is not None:
			got = image.mirror
			if (expect_mirror == "none" and got is not None) or (
				expect_mirror != "none" and got != expect_mirror
			):
				print(
					f"mkosi_config_check: image {image.image} resolved mirror "
					f"{got or 'none'}, want {expect_mirror}",
					file=sys.stderr,
				)
				return 1
	return 0


if __name__ == "__main__":
	sys.exit(main())
