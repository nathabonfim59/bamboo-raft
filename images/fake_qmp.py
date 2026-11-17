#!/usr/bin/env python3
"""Fake QEMU QMP server for images/cpu_probe_test.sh.

Invoked as the probe's QEMU binary, so sys.argv[1:] is the QEMU argv. It
records that argv, serves one QMP connection, and behaves per environment:

  FAKE_ARGV       file to record argv into (one argument per line)
  FAKE_PID        file to record this process's pid into
  FAKE_SOCKET     unix socket path to serve (the probe's qmp.sock)
  FAKE_STATUS     query-status status (default "prelaunch")
  FAKE_OMIT       "1" omits the status field from query-status
  FAKE_OMIT_RUNNING "1" omits the running field from query-status
  FAKE_RUNNING    "true"/"false" for query-status running (default "false")
  FAKE_CPUS       query-cpus-fast shape: "0" (one full entry, default),
                  "none" (empty list), "bare" ([{}]), "nothread" (entry
                  without thread-id), "two" (two full entries), or a comma
                  list of cpu-indexes for full entries
  FAKE_IGNORE_QUIT "1" answers quit but never exits, logs and ignores SIGTERM
  FAKE_TERM_FILE  file to append "TERM" to when SIGTERM arrives (with
                  FAKE_IGNORE_QUIT)
  FAKE_LEAVE_DESCENDANT "1" spawns `sleep 300` into this process group on
                  quit, records its pid in FAKE_DESC, then exits: the leader
                  is reaped while a descendant survives in the owned group
  FAKE_DESC       file to record the left-behind descendant's pid into
  FAKE_SLOW_STATUS seconds to sleep before answering query-status (keeps the
                  probe mid-flight so a test can interrupt the wrapper)
  FAKE_STAGE_FILE file to append "quit-received" to when quit arrives (lets a
                  test interrupt exactly inside the cleanup window)

It speaks exactly the wire protocol internal/node/qmp expects: a greeting
line, then newline-delimited JSON replies whose id matches the request.
"""

import json
import os
import signal
import socket
import sys
import time


def main() -> int:
    argv_path = os.environ.get("FAKE_ARGV")
    if argv_path:
        with open(argv_path, "w") as f:
            for arg in sys.argv[1:]:
                f.write(arg + "\n")

    pid_path = os.environ.get("FAKE_PID")
    if pid_path:
        with open(pid_path, "w") as f:
            f.write(str(os.getpid()) + "\n")

    ignore_quit = os.environ.get("FAKE_IGNORE_QUIT") == "1"
    if ignore_quit:
        term_file = os.environ.get("FAKE_TERM_FILE", "")

        def on_term(_signum, _frame):
            if term_file:
                with open(term_file, "a") as f:
                    f.write("TERM\n")

        signal.signal(signal.SIGTERM, on_term)

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(os.environ["FAKE_SOCKET"])
    server.listen(1)
    conn, _ = server.accept()
    reader = conn.makefile("r")
    writer = conn.makefile("w")

    def send(obj):
        writer.write(json.dumps(obj) + "\n")
        writer.flush()

    send({"QMP": {"version": {"qemu": {"major": 8, "minor": 2, "micro": 2}, "package": ""},
                  "capabilities": ["oob"]}})

    status = os.environ.get("FAKE_STATUS", "prelaunch")
    running = os.environ.get("FAKE_RUNNING", "false") == "true"
    raw_cpus = os.environ.get("FAKE_CPUS", "0")
    if raw_cpus == "none":
        cpus = []
    elif raw_cpus == "bare":
        cpus = [{}]  # an entry with no fields proves nothing
    elif raw_cpus == "nothread":
        cpus = [{"cpu-index": 0, "qom-path": "/machine/cpu[0]"}]
    elif raw_cpus == "two":
        cpus = [{"cpu-index": i, "thread-id": 1000 + i, "qom-path": f"/machine/cpu[{i}]"}
                for i in range(2)]
    else:
        indexes = [int(x) for x in raw_cpus.split(",") if x.strip()]
        cpus = [{"cpu-index": i, "thread-id": 1000 + i, "qom-path": f"/machine/cpu[{i}]"}
                for i in indexes]

    for line in reader:
        try:
            message = json.loads(line)
        except ValueError:
            continue
        command = message.get("execute")
        mid = message.get("id")
        if command == "qmp_capabilities":
            send({"return": {}, "id": mid})
        elif command == "query-status":
            slow = os.environ.get("FAKE_SLOW_STATUS")
            if slow:
                time.sleep(float(slow))
            result = {}
            if os.environ.get("FAKE_OMIT") != "1":
                result["status"] = status
            if os.environ.get("FAKE_OMIT_RUNNING") != "1":
                result["running"] = running
            send({"return": result, "id": mid})
        elif command == "query-cpus-fast":
            send({"return": cpus, "id": mid})
        elif command == "quit":
            stage = os.environ.get("FAKE_STAGE_FILE")
            if stage:
                with open(stage, "a") as f:
                    f.write("quit-received\n")
            send({"return": {}, "id": mid})
            if os.environ.get("FAKE_LEAVE_DESCENDANT") == "1":
                # Leave a background descendant in this process group and exit
                # like a leader that quits while a straggler survives holding
                # the inherited stderr.
                import subprocess
                with open(os.environ.get("FAKE_DESC", ""), "w") as f:
                    f.write(str(subprocess.Popen(["sleep", "300"]).pid) + "\n")
            if not ignore_quit:
                break
        else:
            send({"error": {"class": "CommandNotFound", "desc": f"the fake has no {command}"},
                  "id": mid})

    writer.close()
    reader.close()
    conn.close()
    server.close()
    if ignore_quit:
        # Survive quit and SIGTERM; only SIGKILL ends this process, which is
        # exactly the escalation the probe must take.
        while True:
            time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
