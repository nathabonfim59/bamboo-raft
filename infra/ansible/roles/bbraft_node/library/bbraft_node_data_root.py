#!/usr/bin/python
# Copyright: bamboo-raft. Role-owned module of bbraft_node (A10 fix A).
"""Exclusive acquisition of the bbraft-node v1 data root.

observe (read-only) reports what the configured data root is now: absent,
an empty supervisor-owned 0700 directory, or a current v1 root holding the
exact marker, with the parent, root and marker identities. acquire takes
that observation and acts only if the tree is still exactly what was
observed: an absent root is created by an exclusive mkdirat (EEXIST refuses
and nothing is chowned or chmod'ed), an empty root is marked by an exclusive
no-follow O_EXCL create, a current root is re-proven; on every path the
marker, the root and the parent are fsync'ed again, so an earlier failed
barrier is repaid. verify re-proves the acquired root and marker identities
after the role's other tasks. Everything works through descriptors opened
without following a final symlink. Nothing is ever removed, migrated or
recursively changed; an uncertain publication is preserved and refused.
"""

from __future__ import annotations

import errno
import os
import stat

DOCUMENTATION = r"""
module: bbraft_node_data_root
short_description: Exclusively acquire the bbraft-node v1 data root
options:
  path: {type: path, required: true}
  owner: {type: str, required: true}
  group: {type: str, required: true}
  marker_name: {type: str, required: true}
  marker_content: {type: str, required: true}
  mode: {type: str, required: true, choices: [observe, acquire, verify]}
  expect: {type: dict}
"""

_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class Refused(Exception):
    """The data root is not provably the supervisor's v1 root."""


def _refuse(path, why):
    raise Refused(f"{path} is not a bbraft-node v1 data root: {why}; it is left unchanged "
                  "(a Phase 0.5 or foreign data dir is never reused, chowned or migrated; docs/node.md)")


class Ops:
    """The syscalls acquire uses, behind named fault points for tests."""

    def fault(self, point):
        """Run before the named operation; tests raise here."""

    def mkdir(self, name, dir_fd):
        self.fault("mkdir")
        os.mkdir(name, 0o700, dir_fd=dir_fd)

    def fchown(self, fd, uid, gid, point):
        self.fault(point)
        os.fchown(fd, uid, gid)

    def fchmod(self, fd, mode, point):
        self.fault(point)
        os.fchmod(fd, mode)

    def create_excl(self, name, dir_fd):
        self.fault("marker-create")
        return os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=dir_fd)

    def write(self, fd, data):
        self.fault("marker-write")
        view = memoryview(data)
        while view:
            n = os.write(fd, view)
            view = view[n:]

    def fsync(self, fd, point):
        self.fault(point)
        os.fsync(fd)


def _ident(st):
    return {"dev": st.st_dev, "ino": st.st_ino}


def _same(st, ident):
    return ident is not None and st.st_dev == ident.get("dev") and st.st_ino == ident.get("ino")


def _split(path):
    if not path.startswith("/") or os.path.normpath(path) != path or path == "/":
        raise Refused(f"{path!r}: the data dir must be a clean absolute path")
    parent, name = os.path.split(path)
    if name in ("", ".", ".."):
        raise Refused(f"{path!r}: the data dir must name a directory")
    return parent, name


def _open_parent(path, parent):
    """Open and prove the configured parent (shared by observe, acquire and
    verify): a directory opened without following a final symlink, owned by
    the effective uid and writable by nobody else. acquire relies on that
    trusted parent for the interval between its exclusive mkdir and the open
    that pins the new root. Read-only; nothing above it is checked or
    changed."""
    try:
        fd = os.open(parent, _DIR_FLAGS)
    except FileNotFoundError:
        raise Refused(f"{path}: the configured parent {parent} does not exist (it is never created here)") from None
    except OSError as e:
        raise Refused(f"{path}: the configured parent {parent} is not a directory opened without following a symlink: {e}") from None
    st = os.fstat(fd)
    if st.st_uid != os.geteuid() or st.st_mode & 0o022:
        os.close(fd)
        raise Refused(f"{path}: the configured parent {parent} must be owned by uid {os.geteuid()} and not group- or "
                      f"other-writable (owner {st.st_uid}, mode {stat.S_IMODE(st.st_mode):#o}); nothing was changed")
    return fd


def _stat_name(dir_fd, name):
    try:
        return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _open_root(path, pfd, name):
    try:
        fd = os.open(name, _DIR_FLAGS, dir_fd=pfd)
    except OSError as e:
        if e.errno in (errno.ELOOP, errno.ENOTDIR):
            _refuse(path, "it is a symlink or not a directory")
        raise
    return fd


def _check_root(path, st, uid, gid):
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != uid or st.st_gid != gid or stat.S_IMODE(st.st_mode) != 0o700:
        _refuse(path, f"it exists but is not a {uid}:{gid} 0700 directory")


def _entries(fd, limit=8):
    names = []
    with os.scandir(fd) as it:
        for entry in it:
            names.append(entry.name)
            if len(names) >= limit:
                break
    return names


def _marker_proof(path, rfd, marker_name, content, uid, gid):
    try:
        mfd = os.open(marker_name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=rfd)
    except FileNotFoundError:
        _refuse(path, f"it holds data but no v1 marker {marker_name} (legacy Phase 0.5 data, including root-owned legacy data, or foreign data)")
    except OSError as e:
        _refuse(path, f"its marker {marker_name} cannot be opened without following a symlink: {e}")
    try:
        st = os.fstat(mfd)
        if (not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_uid != uid or st.st_gid != gid
                or stat.S_IMODE(st.st_mode) != 0o600 or st.st_size != len(content)):
            _refuse(path, f"its marker {marker_name} is not a single-link {uid}:{gid} 0600 regular file of {len(content)} bytes")
        data = os.read(mfd, len(content) + 1)
        if data != content:
            _refuse(path, f"its marker {marker_name} does not hold the exact v1 bytes")
        named = _stat_name(rfd, marker_name)
        if named is None or not _same(named, _ident(st)):
            _refuse(path, f"its marker {marker_name} changed while it was read")
        return mfd, st
    except BaseException:
        os.close(mfd)
        raise


def observe(path, uid, gid, marker_name, content, ops):
    """Read-only: absent | empty | current, with identities."""
    parent, name = _split(path)
    pfd = _open_parent(path, parent)
    try:
        out = {"state": "absent", "parent": _ident(os.fstat(pfd)), "root": None, "marker": None}
        named = _stat_name(pfd, name)
        if named is None:
            return out
        rfd = _open_root(path, pfd, name)
        try:
            st = os.fstat(rfd)
            if not _same(st, _ident(named)):
                _refuse(path, "it changed while it was opened")
            _check_root(path, st, uid, gid)
            out["root"] = _ident(st)
            if not _entries(rfd):
                out["state"] = "empty"
                return out
            mfd, mst = _marker_proof(path, rfd, marker_name, content, uid, gid)
            os.close(mfd)
            out["state"], out["marker"] = "current", _ident(mst)
            return out
        finally:
            os.close(rfd)
    finally:
        os.close(pfd)


def _uncertain(path, point, err):
    raise Refused(f"{path}: publishing the v1 marker failed at {point} ({err}); everything is preserved for "
                  "inspection and nothing was removed")


def acquire(path, uid, gid, marker_name, content, expect, ops):
    """Acquire exactly the observed root (expect) and repay every barrier.

    The authority for a created root is the directory this call's exclusive
    mkdir made, pinned by descriptor and identity before any later boundary;
    the authority for a created marker is the descriptor of its O_EXCL
    create. After every mutation and every barrier the configured names must
    still be those inodes, and a replacement is refused, never adopted, and
    left exactly as it is. The interval between mkdir and the open that pins
    the new directory is not atomic (there is no mkdir-and-open syscall): the
    configured parent is trusted to be writable by root only, and the pinned
    directory must still be the euid-owned, single-link, empty directory
    under that name.
    """
    parent, name = _split(path)
    state = (expect or {}).get("state")
    if state not in ("absent", "empty", "current"):
        raise Refused(f"{path}: acquire needs an observation, got {expect!r}")
    pfd = _open_parent(path, parent)
    fds = [pfd]
    try:
        if not _same(os.fstat(pfd), expect.get("parent")):
            _refuse(path, f"its parent {parent} is not the observed directory")
        created = marked = False
        if state == "absent":
            try:
                ops.mkdir(name, pfd)
            except FileExistsError:
                _refuse(path, "it appeared after it was observed absent")
            except OSError as e:
                raise Refused(f"{path}: create the data root: {e}") from None
            created = True
        try:
            rfd = _open_root(path, pfd, name)
        except FileNotFoundError:
            _refuse(path, "it disappeared")
        fds.append(rfd)
        st = os.fstat(rfd)
        root_id = _ident(st)
        marker_id = None

        def require(where):
            named = _stat_name(pfd, name)
            if named is None or not _same(named, root_id):
                _refuse(path, f"it was replaced ({where}); the replacement is preserved")
            if marker_id is not None:
                m = _stat_name(rfd, marker_name)
                if m is None or not _same(m, marker_id):
                    _refuse(path, f"its marker {marker_name} was replaced ({where}); the replacement is preserved")

        require("while it was opened")
        if created:
            # Only the directory this attempt's exclusive mkdir made, pinned
            # above, is ever chowned or chmod'ed, and only while its name is
            # still that directory.
            if st.st_uid != os.geteuid() or st.st_nlink != 2 or _entries(rfd):
                _refuse(path, "the directory just created is not the empty one this attempt made")
            ops.fault("after-mkdir")
            require("after mkdir")
            try:
                ops.fchown(rfd, uid, gid, "root-fchown")
                require("at its chown")
                ops.fchmod(rfd, 0o700, "root-fchmod")
                require("at its chmod")
            except OSError as e:
                raise Refused(f"{path}: set the new data root's owner and mode: {e}") from None
            st = os.fstat(rfd)
        elif not _same(st, expect.get("root")):
            _refuse(path, "it is not the observed directory (its inode was replaced)")
        _check_root(path, st, uid, gid)
        if state == "current":
            mfd, mst = _marker_proof(path, rfd, marker_name, content, uid, gid)
            fds.append(mfd)
            if not _same(mst, expect.get("marker")):
                _refuse(path, f"its marker {marker_name} is not the observed marker (its inode was replaced)")
            marker_id, sync_fd = _ident(mst), mfd
        else:
            if _entries(rfd):
                _refuse(path, "it gained entries after it was observed empty")
            try:
                wfd = ops.create_excl(marker_name, rfd)
            except FileExistsError:
                _refuse(path, f"a {marker_name} appeared after the root was observed empty")
            except OSError as e:
                raise Refused(f"{path}: create the v1 marker: {e}") from None
            fds.append(wfd)
            marked = True
            marker_id, sync_fd = _ident(os.fstat(wfd)), wfd
            require("at its create")
            try:
                ops.write(wfd, content)
                require("at its write")
                ops.fchown(wfd, uid, gid, "marker-fchown")
                require("at its chown")
                ops.fchmod(wfd, 0o600, "marker-fchmod")
                require("at its chmod")
            except OSError as e:
                _uncertain(path, "write", e)
            # The proof reads the named marker; it must be the created inode.
            ops.fault("marker-proof")
            mfd, mst = _marker_proof(path, rfd, marker_name, content, uid, gid)
            fds.append(mfd)
            if not _same(mst, marker_id):
                _refuse(path, f"its marker {marker_name} is not the inode this attempt created; the replacement is preserved")
            if sorted(_entries(rfd)) != [marker_name]:
                _refuse(path, "it gained entries while it was marked")
        for fd, point in ((sync_fd, "marker-fsync"), (rfd, "root-fsync"), (pfd, "parent-fsync")):
            try:
                ops.fsync(fd, point)
            except OSError as e:
                _uncertain(path, "fsync", e)
            require(f"at {point}")
        require("after the barriers")
        out_state = "created" if created else ("marked" if marked else "current")
        return {"state": out_state, "changed": created or marked, "parent": _ident(os.fstat(pfd)),
                "root": root_id, "marker": marker_id}
    finally:
        for fd in reversed(fds):
            os.close(fd)


def verify(path, uid, gid, marker_name, content, expect, ops):
    """Re-prove the acquired root and marker identities (read-only)."""
    got = observe(path, uid, gid, marker_name, content, ops)
    if got["state"] != "current" or not expect:
        _refuse(path, f"it is {got['state']}, not the acquired v1 root")
    for key in ("parent", "root", "marker"):
        if got[key] != {"dev": (expect.get(key) or {}).get("dev"), "ino": (expect.get(key) or {}).get("ino")}:
            _refuse(path, f"its {key} is not the acquired one (replaced)")
    return got


def main():  # pragma: no cover - exercised through the role's Ansible tests
    import grp
    import pwd

    from ansible.module_utils.basic import AnsibleModule

    module = AnsibleModule(
        argument_spec={
            "path": {"type": "path", "required": True},
            "owner": {"type": "str", "required": True},
            "group": {"type": "str", "required": True},
            "marker_name": {"type": "str", "required": True},
            "marker_content": {"type": "str", "required": True, "no_log": False},
            "mode": {"type": "str", "required": True, "choices": ["observe", "acquire", "verify"]},
            "expect": {"type": "dict"},
        },
        supports_check_mode=True,
    )
    p = module.params
    try:
        uid = pwd.getpwnam(p["owner"]).pw_uid
        gid = grp.getgrnam(p["group"]).gr_gid
    except KeyError as e:
        module.fail_json(msg=f"unknown owner or group: {e}")
    content = p["marker_content"].encode()
    ops = Ops()
    try:
        if p["mode"] == "observe" or (p["mode"] == "acquire" and module.check_mode):
            result = observe(p["path"], uid, gid, p["marker_name"], content, ops)
            result["changed"] = False
        elif p["mode"] == "acquire":
            result = acquire(p["path"], uid, gid, p["marker_name"], content, p["expect"], ops)
        else:
            result = verify(p["path"], uid, gid, p["marker_name"], content, p["expect"], ops)
            result["changed"] = False
    except Refused as e:
        module.fail_json(msg=str(e))
    except OSError as e:
        module.fail_json(msg=f"{p['path']} is not a bbraft-node v1 data root: {e}")
    module.exit_json(**result)


if __name__ == "__main__":
    main()
