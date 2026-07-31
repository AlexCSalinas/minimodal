"""Make `worker` and `minimodal` importable without installing anything."""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

for path in (ROOT, os.path.join(ROOT, "sdk")):
    if path not in sys.path:
        sys.path.insert(0, path)
