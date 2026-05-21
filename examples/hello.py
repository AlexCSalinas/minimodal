"""Phase 2 hello world. Run after the orchestrator + a worker are up:

    python examples/hello.py
"""

from minimodal import App

app = App("hello")


@app.function
def greet(name: str) -> str:
    return f"hi {name}"


if __name__ == "__main__":
    print(greet.remote("alex"))
