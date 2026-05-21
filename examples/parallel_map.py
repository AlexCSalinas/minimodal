"""Fan-out: submit N invocations, then collect results. Phase 2."""

import time

from minimodal import App

app = App("parallel-map")


@app.function
def square(x: int) -> int:
    return x * x


if __name__ == "__main__":
    t0 = time.monotonic()
    futures = [square.spawn(i) for i in range(20)]
    results = [f.get() for f in futures]
    print(f"sum={sum(results)} in {(time.monotonic() - t0) * 1000:.1f}ms")
