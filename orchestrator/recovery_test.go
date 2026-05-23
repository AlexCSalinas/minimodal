package main

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryInterceptor_CatchesPanic(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Boom"}
	handler := func(_ context.Context, _ any) (any, error) {
		panic("kaboom")
	}

	_, err := recoveryUnaryInterceptor(context.Background(), nil, info, handler)
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got %v", err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal code, got %s", st.Code())
	}
}

func TestRecoveryInterceptor_CatchesPanicOnNilDeref(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Nil"}
	handler := func(_ context.Context, _ any) (any, error) {
		var p *struct{ X int }
		_ = p.X // intentional nil deref panic
		return nil, nil
	}

	_, err := recoveryUnaryInterceptor(context.Background(), nil, info, handler)
	if err == nil {
		t.Fatal("expected error from nil-deref panic")
	}
}

func TestRecoveryInterceptor_PassesThroughNormalReturn(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Ok"}
	want := "result-payload"
	handler := func(_ context.Context, _ any) (any, error) {
		return want, nil
	}

	resp, err := recoveryUnaryInterceptor(context.Background(), nil, info, handler)
	if err != nil {
		t.Errorf("normal handler should not produce error, got %v", err)
	}
	if resp != want {
		t.Errorf("response not passed through: got %v want %v", resp, want)
	}
}

func TestRecoveryInterceptor_PassesThroughNormalError(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Err"}
	want := errors.New("expected business error")
	handler := func(_ context.Context, _ any) (any, error) {
		return nil, want
	}

	_, err := recoveryUnaryInterceptor(context.Background(), nil, info, handler)
	if !errors.Is(err, want) {
		t.Errorf("normal error should pass through unchanged, got %v", err)
	}
}
