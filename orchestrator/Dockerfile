FROM golang:1.25-alpine AS build

RUN apk add --no-cache protoc make protobuf-dev git

WORKDIR /src

# Install Go protoc plugins.
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY orchestrator/go.mod orchestrator/go.sum* ./orchestrator/
RUN cd orchestrator && go mod download || true

COPY proto/ ./proto/
COPY orchestrator/ ./orchestrator/

ENV PATH=$PATH:/root/go/bin
RUN mkdir -p orchestrator/pb \
 && protoc \
    --proto_path=proto \
    --go_out=orchestrator/pb --go_opt=paths=source_relative \
    --go-grpc_out=orchestrator/pb --go-grpc_opt=paths=source_relative \
    proto/minimodal.proto

RUN cd orchestrator && go build -o /out/orchestrator .

FROM alpine:3.20
RUN mkdir -p /data
COPY --from=build /out/orchestrator /usr/local/bin/orchestrator
EXPOSE 50051 8080
ENTRYPOINT ["/usr/local/bin/orchestrator"]
