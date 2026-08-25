"""Live log streaming demo. Run after the orchestrator + a worker are up:

    python examples/streaming_logs.py

Each step's print() appears on THIS terminal while the function is still
running on the worker — then the return value arrives, pushed over the same
stream.
"""

from minimodal import App

app = App("streaming-demo")


@app.function
def train(steps: int) -> str:
    import time

    for i in range(steps):
        print(f"step {i + 1}/{steps} loss={1.0 / (i + 1):.3f}")
        time.sleep(0.3)
    return "model trained"


if __name__ == "__main__":
    print(train.remote(5))
