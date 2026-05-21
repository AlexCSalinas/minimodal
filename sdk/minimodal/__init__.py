"""minimodal — Python client SDK.

Usage:

    from minimodal import App

    app = App("my-app")

    @app.function
    def greet(name: str) -> str:
        return f"hi {name}"

    if __name__ == "__main__":
        print(greet.remote("alex"))
"""

from minimodal.app import App, Func  # noqa: F401

__version__ = "0.1.0"
__all__ = ["App", "Func"]
