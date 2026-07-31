"""Heavy-import example. Demonstrates cold start impact: importing numpy in a
fresh interpreter takes ~150ms on a modern machine; the warm fork pool reuses
the already-imported numpy and skips that cost entirely.
"""

import numpy as np
from minimodal import App

app = App("numpy-matmul")


@app.function
def matmul(n: int) -> float:
    a = np.random.rand(n, n)
    b = np.random.rand(n, n)
    return float(np.linalg.norm(a @ b))


if __name__ == "__main__":
    print(matmul.remote(256))
