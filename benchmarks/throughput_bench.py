"""Phase 5 sustained-throughput benchmark. Submits invocations as fast as
possible for T seconds and reports invocations/second + worker utilization.
"""

import os
import time

from minimodal import App

app = App("throughput-bench")


@app.function
def noop() -> int:
    return 1


if __name__ == "__main__":
    duration_s = float(os.environ.get("MINIMODAL_BENCH_S", "10"))
    futures = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < duration_s:
        futures.append(noop.spawn())
    for f in futures:
        f.get()
    elapsed = time.monotonic() - t0
    print(f"n={len(futures)} duration={elapsed:.1f}s rps={len(futures) / elapsed:.0f}")
