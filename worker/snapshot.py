"""CRIU integration hooks. Phase 3 stubs; CRIU is the third cold-start mode.

Background
----------
CRIU (Checkpoint/Restore In Userspace) lets you dump a process's full state
(memory pages, open files, sockets, registers) to disk and restore it later.
For Python cold starts the win is huge: skip the import step entirely by
restoring a process that already has numpy/torch/etc. loaded.

The minimal happy path is:

  1. Start a fresh Python interpreter, import the deps, evaluate any one-time
     init code (model load, JIT warmup).
  2. Call into the CRIU binary (via libcriu or `criu dump`) to snapshot the
     process.
  3. On invocation, run `criu restore --restore-detached` to spawn a fresh
     copy of that snapshotted process.
  4. The restored process inherits memory pages lazily via userfaultfd, so the
     restore call returns in ~ms even for processes with hundreds of MB of
     resident pages.

Where to plug in
----------------
checkpoint_process(pid, image_dir): wraps `criu dump --tree <pid> -D <dir>`
restore_process(image_dir):         wraps `criu restore -D <dir> --restore-detached`

We invoke as subprocesses (rather than libcriu) for portability; libcriu is a
small win that we can graduate to later.

Why this is stubbed in v1
-------------------------
CRIU only works on Linux with specific kernel features (CONFIG_CHECKPOINT_RESTORE,
userfaultfd). Local dev on macOS can't exercise it without a Linux VM. We keep
the seams clear so a Linux deploy can drop in real implementations without
ripping up the call graph.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


class CRIUNotAvailable(RuntimeError):
    pass


@dataclass
class SnapshotConfig:
    image_dir: Path
    criu_binary: str = "criu"


def criu_available() -> bool:
    """Cheap check for CRIU availability — does the binary exist + can we exec it."""
    try:
        subprocess.run(
            ["criu", "--version"],
            check=True, capture_output=True, timeout=2.0,
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


def checkpoint_process(pid: int, config: SnapshotConfig) -> None:
    """Dump the live process at `pid` into `config.image_dir`.

    TODO(phase-3): real implementation. Should:
      - Ensure config.image_dir exists and is empty.
      - Run `criu dump --tree <pid> -D <dir> --shell-job --leave-running`.
      - Capture stderr; on non-zero exit raise with the criu error log.
    """
    if not criu_available():
        raise CRIUNotAvailable("criu binary not on PATH")
    raise NotImplementedError("checkpoint_process stubbed in Phase 1")


def restore_process(config: SnapshotConfig) -> int:
    """Restore from `config.image_dir`, return restored PID.

    TODO(phase-3): real implementation. Should:
      - Run `criu restore -D <dir> --restore-detached --shell-job`.
      - Parse PID from stdout (or read pidfile).
      - Set up a connection back to the orchestrator from the restored process.
    """
    if not criu_available():
        raise CRIUNotAvailable("criu binary not on PATH")
    raise NotImplementedError("restore_process stubbed in Phase 1")
