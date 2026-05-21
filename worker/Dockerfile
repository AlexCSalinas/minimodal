FROM python:3.12-slim

WORKDIR /app

COPY worker/requirements.txt ./worker/requirements.txt
RUN pip install --no-cache-dir -r worker/requirements.txt

COPY proto/ ./proto/
COPY worker/ ./worker/
COPY sdk/ ./sdk/

# Generate Python proto stubs at build time.
RUN python -m grpc_tools.protoc \
    --proto_path=proto \
    --python_out=worker/pb \
    --grpc_python_out=worker/pb \
    proto/minimodal.proto \
 && touch worker/pb/__init__.py \
 && python -m grpc_tools.protoc \
    --proto_path=proto \
    --python_out=sdk/minimodal/pb \
    --grpc_python_out=sdk/minimodal/pb \
    proto/minimodal.proto \
 && touch sdk/minimodal/pb/__init__.py

RUN pip install -e sdk

ENV PYTHONPATH=/app:/app/sdk
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "worker.worker"]
