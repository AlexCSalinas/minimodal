package main

import (
	"context"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recoveryUnaryInterceptor catches panics in gRPC handlers so a buggy request
// path can't take down the whole orchestrator. The full stack is logged for
// debugging; the client sees a clean Internal error.
func recoveryUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("PANIC in gRPC handler",
				"method", info.FullMethod,
				"panic", r,
				"stack", string(debug.Stack()))
			err = status.Errorf(codes.Internal, "internal server panic")
		}
	}()
	return handler(ctx, req)
}
