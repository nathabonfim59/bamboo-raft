"""Unit tests for the bbraft_node_data_root module's core (A10 role fix A).

Run directly with python3; no Ansible is needed:

    python3 roles/bbraft_node/tests/test_data_root.py

The module's observe/acquire/verify functions run on temporary trees with
an Ops fake that records syscalls and injects faults, so publish and sync
failures, identity replacements and retries are covered without privilege.
"""

from __future__ import annotations

import errno
import importlib.util
import os
import pathlib
import stat
import tempfile
import unittest

MODULE_PATH = pathlib.Path(__file__).resolve().parent.parent / "library" / "bbraft_node_data_root.py"
SPEC = importlib.util.spec_from_file_location("bbraft_node_data_root", MODULE_PATH)
DR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DR)

MARKER = ".bbraft-node-data-v1"
CONTENT = b"bbraft-node data root v1\n"


class FaultOps(DR.Ops):
    """Records named points, raises OSError(EIO) at the chosen ones and runs
    an action (a concurrent change) at others."""

    def __init__(self, faults=(), actions=None):
        super().__init__()
        self.faults = set(faults)
        self.actions = dict(actions or {})
        self.points = []

    def fault(self, point):
        self.points.append(point)
        if point in self.actions:
            self.actions.pop(point)()
        if point in self.faults:
            raise OSError(errno.EIO, f"injected fault at {point}")


def snapshot(root):
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        for name in sorted([dirpath] + [os.path.join(dirpath, n) for n in dirnames + filenames]):
            st = os.lstat(name)
            data = b""
            if stat.S_ISREG(st.st_mode):
                data = pathlib.Path(name).read_bytes()
            out.append((os.path.relpath(name, root), st.st_ino, st.st_uid, st.st_gid, stat.S_IMODE(st.st_mode), st.st_nlink, data))
    return sorted(set(out))


class DataRootTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.parent = self.tmp.name
        self.path = os.path.join(self.parent, "data")
        self.uid, self.gid = os.geteuid(), os.getegid()
        # Subtests call setUp again for a fresh tree; each tree is cleaned.
        self.addCleanup(self.tmp.cleanup)

    def observe(self, ops=None):
        return DR.observe(self.path, self.uid, self.gid, MARKER, CONTENT, ops or DR.Ops())

    def acquire(self, expect, ops=None):
        return DR.acquire(self.path, self.uid, self.gid, MARKER, CONTENT, expect, ops or DR.Ops())

    def verify(self, expect, ops=None):
        return DR.verify(self.path, self.uid, self.gid, MARKER, CONTENT, expect, ops or DR.Ops())

    def write_marker(self, data=CONTENT, mode=0o600):
        p = os.path.join(self.path, MARKER)
        pathlib.Path(p).write_bytes(data)
        os.chmod(p, mode)

    def ident(self, path):
        st = os.lstat(path)
        return (st.st_dev, st.st_ino)

    def replace_marker(self, aside):
        """Replace the marker by an identical-looking new inode. The old one
        is renamed to an owned sibling outside the root and kept, so the
        filesystem cannot hand its inode number to the replacement (ext4
        reuses a just-freed inode at once); returns the aside path."""
        m = os.path.join(self.path, MARKER)
        old = self.ident(m)
        aside = os.path.join(self.parent, aside)
        os.rename(m, aside)
        self.write_marker()
        self.assertNotEqual(self.ident(m), old, "the replacement marker reused the old identity")
        self.assertEqual(self.ident(aside), old)
        return aside

    def replace_root(self, aside):
        """Replace the root by an identical-looking new directory; the old
        one is kept under aside, and its identity must differ."""
        old = self.ident(self.path)
        aside = os.path.join(self.parent, aside)
        os.rename(self.path, aside)
        os.mkdir(self.path, 0o700)
        self.write_marker()
        self.assertNotEqual(self.ident(self.path), old, "the replacement root reused the old identity")
        self.assertEqual(self.ident(aside), old)
        return aside

    def assert_marker(self):
        p = os.path.join(self.path, MARKER)
        st = os.lstat(p)
        self.assertTrue(stat.S_ISREG(st.st_mode))
        self.assertEqual(stat.S_IMODE(st.st_mode), 0o600)
        self.assertEqual(st.st_nlink, 1)
        self.assertEqual(pathlib.Path(p).read_bytes(), CONTENT)
        self.assertEqual(stat.S_IMODE(os.lstat(self.path).st_mode), 0o700)

    def test_observe_states(self):
        self.assertEqual(self.observe()["state"], "absent")
        os.mkdir(self.path, 0o700)
        obs = self.observe()
        self.assertEqual(obs["state"], "empty")
        self.assertEqual(obs["root"]["ino"], os.lstat(self.path).st_ino)
        self.write_marker()
        obs = self.observe()
        self.assertEqual(obs["state"], "current")
        self.assertEqual(obs["marker"]["ino"], os.lstat(os.path.join(self.path, MARKER)).st_ino)

    def test_observe_is_read_only(self):
        os.mkdir(self.path, 0o755)
        before = snapshot(self.parent)
        with self.assertRaises(DR.Refused):
            self.observe()
        self.assertEqual(snapshot(self.parent), before)

    def test_acquire_absent_creates_marks_and_syncs(self):
        ops = FaultOps()
        got = self.acquire(self.observe(), ops)
        self.assertEqual(got["state"], "created")
        self.assertTrue(got["changed"])
        self.assert_marker()
        for point in ["mkdir", "root-fchown", "marker-create", "marker-fsync", "root-fsync", "parent-fsync"]:
            self.assertIn(point, ops.points)

    def test_acquire_empty_and_current_repay_every_barrier(self):
        os.mkdir(self.path, 0o700)
        ops = FaultOps()
        self.assertEqual(self.acquire(self.observe(), ops)["state"], "marked")
        ops = FaultOps()
        got = self.acquire(self.observe(), ops)
        self.assertEqual(got["state"], "current")
        self.assertFalse(got["changed"])
        for point in ["marker-fsync", "root-fsync", "parent-fsync"]:
            self.assertIn(point, ops.points, f"current acquire must repay {point}")
        self.assertNotIn("root-fchown", ops.points)
        self.assertNotIn("marker-create", ops.points)

    def test_absent_then_planted_directory_is_untouched(self):
        obs = self.observe()
        os.mkdir(self.path, 0o755)
        pathlib.Path(self.path, "sentinel").write_bytes(b"x")
        before = snapshot(self.parent)
        ops = FaultOps()
        with self.assertRaises(DR.Refused):
            self.acquire(obs, ops)
        self.assertEqual(snapshot(self.parent), before)
        self.assertNotIn("root-fchown", ops.points)

    def test_empty_then_planted_entries_are_refused_unchanged(self):
        for plant in ["legacy", MARKER]:
            with self.subTest(plant=plant):
                self.setUp()
                os.mkdir(self.path, 0o700)
                obs = self.observe()
                pathlib.Path(self.path, plant).write_bytes(b"bbraft-node data root v0\n")
                os.chmod(os.path.join(self.path, plant), 0o600)
                before = snapshot(self.parent)
                with self.assertRaises(DR.Refused):
                    self.acquire(obs)
                self.assertEqual(snapshot(self.parent), before)

    def test_identity_replacements_are_refused(self):
        os.mkdir(self.path, 0o700)
        self.write_marker()
        obs = self.observe()
        for name, change in {
            "root": lambda: self.replace_root("data.old"),
            "marker": lambda: self.replace_marker("marker.old"),
        }.items():
            with self.subTest(replaced=name):
                aside = change()
                old = self.ident(aside)
                before = snapshot(self.parent)
                with self.assertRaises(DR.Refused):
                    self.acquire(obs)
                self.assertEqual(snapshot(self.parent), before)
                self.assertEqual(self.ident(aside), old)  # the replaced inode is still held
                obs = self.observe()  # the new state is acquirable afresh
                self.acquire(obs)

    def test_parent_identity_is_part_of_the_expectation(self):
        obs = self.observe()
        os.rename(self.parent, self.parent + ".moved")
        os.mkdir(self.parent)
        try:
            with self.assertRaises(DR.Refused):
                self.acquire(obs)
            self.assertFalse(os.path.exists(self.path))
        finally:
            os.rmdir(self.parent)
            os.rename(self.parent + ".moved", self.parent)

    def test_missing_or_symlinked_parent_is_refused(self):
        path = os.path.join(self.parent, "missing", "data")
        with self.assertRaises(DR.Refused):
            DR.observe(path, self.uid, self.gid, MARKER, CONTENT, DR.Ops())
        self.assertFalse(os.path.exists(os.path.join(self.parent, "missing")))
        real = os.path.join(self.parent, "real")
        os.mkdir(real)
        os.symlink(real, os.path.join(self.parent, "link"))
        with self.assertRaises(DR.Refused):
            DR.observe(os.path.join(self.parent, "link", "data"), self.uid, self.gid, MARKER, CONTENT, DR.Ops())

    def test_sync_faults_are_retryable(self):
        for point in ["root-fsync", "parent-fsync", "marker-fsync"]:
            with self.subTest(point=point):
                self.setUp()
                with self.assertRaises(DR.Refused):
                    self.acquire(self.observe(), FaultOps([point]))
                self.assert_marker()  # preserved, not removed
                ops = FaultOps()
                self.assertEqual(self.acquire(self.observe(), ops)["state"], "current")
                self.assertIn(point, ops.points)

    def test_mkdir_and_ownership_faults_are_retryable(self):
        for point in ["mkdir", "root-fchown"]:
            with self.subTest(point=point):
                self.setUp()
                with self.assertRaises(DR.Refused):
                    self.acquire(self.observe(), FaultOps([point]))
                got = self.acquire(self.observe())
                self.assertIn(got["state"], ["created", "marked"])
                self.assert_marker()

    def test_uncertain_marker_publication_is_preserved(self):
        for point in ["marker-write", "marker-fchown"]:
            with self.subTest(point=point):
                self.setUp()
                os.mkdir(self.path, 0o700)
                with self.assertRaises(DR.Refused):
                    self.acquire(self.observe(), FaultOps([point]))
                self.assertTrue(os.path.lexists(os.path.join(self.path, MARKER)), "the uncertain marker was removed")

    def test_absent_then_planted_empty_directory_is_not_chmod_ed(self):
        obs = self.observe()
        os.mkdir(self.path, 0o755)
        os.chmod(self.path, 0o755)
        before = snapshot(self.parent)
        ops = FaultOps()
        with self.assertRaises(DR.Refused):
            self.acquire(obs, ops)
        self.assertEqual(snapshot(self.parent), before)
        self.assertNotIn("root-fchown", ops.points)

    def test_swap_right_after_mkdir_is_never_chowned(self):
        def swap():
            os.rmdir(self.path)
            os.mkdir(self.path, 0o755)
            pathlib.Path(self.path, "foreign").write_bytes(b"x")
        ops = FaultOps(actions={"after-mkdir": swap})
        with self.assertRaises(DR.Refused):
            self.acquire(self.observe(), ops)
        self.assertNotIn("root-fchown", ops.points)
        self.assertEqual(stat.S_IMODE(os.lstat(self.path).st_mode), 0o755)
        self.assertFalse(os.path.lexists(os.path.join(self.path, MARKER)))

    def test_marker_planted_right_before_the_exclusive_create(self):
        os.mkdir(self.path, 0o700)
        def plant():
            pathlib.Path(self.path, MARKER).write_bytes(b"bbraft-node data root v0\n")
            os.chmod(os.path.join(self.path, MARKER), 0o600)
        ops = FaultOps(actions={"marker-create": plant})
        with self.assertRaises(DR.Refused):
            self.acquire(self.observe(), ops)
        self.assertEqual(pathlib.Path(self.path, MARKER).read_bytes(), b"bbraft-node data root v0\n")

    def test_root_replaced_but_marker_inode_moved_along(self):
        os.mkdir(self.path, 0o700)
        self.write_marker()
        obs = self.observe()
        os.rename(self.path, self.path + ".old")
        os.mkdir(self.path, 0o700)
        os.rename(os.path.join(self.path + ".old", MARKER), os.path.join(self.path, MARKER))
        with self.assertRaises(DR.Refused):
            self.acquire(obs)

    # --- A10 role fix round 2: created-inode authority -------------------

    def fds(self):
        return len(os.listdir("/proc/self/fd"))

    def refuse_with_swap(self, setup, point, swap, check, reason=None):
        """Run acquire with swap at point; it must refuse, leave the swapped
        state exactly as the swap made it, and leak no descriptor."""
        setup()
        obs = self.observe()
        state = {}

        def action():
            swap(state)
            state["after"] = snapshot(self.parent)

        before_fds = self.fds()
        with self.assertRaises(DR.Refused) as refused:
            self.acquire(obs, FaultOps(actions={point: action}))
        if reason is not None:
            # The boundary's own re-proof must catch it, not a later check.
            self.assertIn(reason, str(refused.exception))
        self.assertEqual(self.fds(), before_fds, "descriptor leak on refusal")
        self.assertIn("after", state, f"the {point} boundary was never reached")
        # Only this attempt's own moved-aside inode may still change (a write
        # through its held descriptor); every replacement and other entry is
        # preserved exactly.
        ours = state.get("ours")
        keep = lambda snap: [row for row in snap if row[1] != ours]
        self.assertEqual(keep(snapshot(self.parent)), keep(state["after"]), f"acquire changed the tree after the {point} swap")
        check(state)

    def swap_root(self, state):
        os.rename(self.path, self.path + ".ours")
        os.mkdir(self.path, 0o755)
        os.chmod(self.path, 0o755)
        state["planted"] = os.lstat(self.path).st_ino

    def swap_marker(self, state):
        m = os.path.join(self.path, MARKER)
        # Moved outside the root, so the root's entry set stays {marker} and
        # only the inode identity can tell the replacement apart.
        state["ours"] = os.lstat(m).st_ino
        os.rename(m, self.path + ".marker-aside")
        pathlib.Path(m).write_bytes(CONTENT)
        os.chmod(m, 0o600)
        state["planted"] = os.lstat(m).st_ino

    def test_created_root_swapped_after_mkdir_is_never_adopted(self):
        def check(state):
            st = os.lstat(self.path)
            self.assertEqual(st.st_ino, state["planted"])
            self.assertEqual(stat.S_IMODE(st.st_mode), 0o755)
            self.assertFalse(os.path.lexists(os.path.join(self.path, MARKER)))
        self.refuse_with_swap(lambda: None, "after-mkdir", self.swap_root, check)

    def test_created_root_swapped_at_chown_or_chmod(self):
        for point in ["root-fchown", "root-fchmod"]:
            with self.subTest(point=point):
                self.setUp()
                self.refuse_with_swap(lambda: None, point, self.swap_root,
                                      lambda state: self.assertEqual(stat.S_IMODE(os.lstat(self.path).st_mode), 0o755))

    def test_created_marker_swapped_is_never_adopted(self):
        for point, reason in {"marker-write": "at its write", "marker-fchown": "at its chown", "marker-fchmod": "at its chmod",
                              "marker-proof": "is not the inode this attempt created", "marker-fsync": "at marker-fsync"}.items():
            with self.subTest(point=point):
                self.setUp()
                def check(state):
                    m = os.path.join(self.path, MARKER)
                    self.assertEqual(os.lstat(m).st_ino, state["planted"])
                    self.assertTrue(os.path.lexists(self.path + ".marker-aside"))
                self.refuse_with_swap(lambda: os.mkdir(self.path, 0o700), point, self.swap_marker, check, reason)

    def test_swaps_at_root_and_parent_barriers(self):
        for point in ["root-fsync", "parent-fsync"]:
            for name, swap in {"root": self.swap_root, "marker": self.swap_marker}.items():
                with self.subTest(point=point, swapped=name):
                    self.setUp()
                    self.refuse_with_swap(lambda: os.mkdir(self.path, 0o700), point, swap, lambda state: None)

    def test_current_marker_swapped_at_a_barrier(self):
        for point in ["marker-fsync", "root-fsync", "parent-fsync"]:
            with self.subTest(point=point):
                self.setUp()
                def setup():
                    os.mkdir(self.path, 0o700)
                    self.write_marker()
                self.refuse_with_swap(setup, point, self.swap_marker, lambda state: None)

    def test_returned_identities_name_the_published_inodes(self):
        got = self.acquire(self.observe())
        st = os.lstat(os.path.join(self.path, MARKER))
        self.assertEqual(got["marker"], {"dev": st.st_dev, "ino": st.st_ino})
        self.assertEqual(got["root"]["ino"], os.lstat(self.path).st_ino)

    def test_no_descriptor_leak_on_success_and_retry(self):
        before = self.fds()
        self.acquire(self.observe())
        self.acquire(self.observe())
        self.verify(self.acquire(self.observe()))
        self.assertEqual(self.fds(), before)

    # --- A10 role fix round 2b: trusted parent proof ----------------------

    def test_trusted_parent_positive_control(self):
        os.chmod(self.parent, 0o755)
        self.assertEqual(self.observe()["state"], "absent")
        self.assertEqual(self.acquire(self.observe())["state"], "created")

    def test_group_or_other_writable_parent_is_refused_unchanged(self):
        for mode in (0o775, 0o757, 0o777, 0o1777):
            with self.subTest(mode=oct(mode)):
                self.setUp()
                os.mkdir(self.path, 0o700)
                self.write_marker()
                os.chmod(self.parent, mode)
                before = snapshot(self.parent)
                for call in (lambda: self.observe(),
                             lambda: self.acquire({"state": "absent", "parent": {"dev": 0, "ino": 0}}),
                             lambda: self.verify({"parent": {}, "root": {}, "marker": {}})):
                    with self.assertRaises(DR.Refused) as refused:
                        call()
                    self.assertIn("parent", str(refused.exception))
                self.assertEqual(snapshot(self.parent), before)
                os.chmod(self.parent, 0o700)

    def test_parent_writable_after_observe_is_refused_before_mkdir(self):
        obs = self.observe()
        os.chmod(self.parent, 0o777)
        with self.assertRaises(DR.Refused):
            self.acquire(obs)
        self.assertFalse(os.path.lexists(self.path))
        os.chmod(self.parent, 0o700)

    @unittest.skipIf(os.geteuid() == 0, "needs a parent owned by another uid")
    def test_foreign_owned_parent_is_refused_without_mutation(self):
        parent = "/usr"  # owned by root, never by the unprivileged test user
        self.assertNotEqual(os.lstat(parent).st_uid, os.geteuid())
        path = os.path.join(parent, "bbraft-data-root-test-never-created")
        with self.assertRaises(DR.Refused) as refused:
            DR.observe(path, self.uid, self.gid, MARKER, CONTENT, DR.Ops())
        self.assertIn("parent", str(refused.exception))
        with self.assertRaises(DR.Refused):
            DR.acquire(path, self.uid, self.gid, MARKER, CONTENT, {"state": "absent", "parent": {"dev": 0, "ino": 0}}, DR.Ops())
        self.assertFalse(os.path.lexists(path))

    def test_verify(self):
        acquired = self.acquire(self.observe())
        self.assertEqual(self.verify(acquired)["state"], "current")
        def root_marker_moved_along():
            # Only the root is new; the acquired marker inode moves into it.
            old = self.ident(self.path)
            aside = os.path.join(self.parent, "data.moved")
            os.rename(self.path, aside)
            os.mkdir(self.path, 0o700)
            os.rename(os.path.join(aside, MARKER), os.path.join(self.path, MARKER))
            self.assertNotEqual(self.ident(self.path), old, "the replacement root reused the old identity")
            self.assertEqual(self.ident(aside), old)
            return aside

        for name, replace in [
            ("marker", lambda: self.replace_marker("marker.old")),
            ("root", lambda: self.replace_root("data.old")),
            ("root, marker moved along", root_marker_moved_along),
        ]:
            with self.subTest(replaced=name):
                aside = replace()
                old = self.ident(aside)
                before = snapshot(self.parent)
                with self.assertRaises(DR.Refused):
                    self.verify(acquired)
                self.assertEqual(snapshot(self.parent), before)
                self.assertEqual(self.ident(aside), old)  # held through verification
                acquired = self.acquire(self.observe())  # the new state is acquirable afresh
                self.assertEqual(self.verify(acquired)["state"], "current")


if __name__ == "__main__":
    unittest.main()
