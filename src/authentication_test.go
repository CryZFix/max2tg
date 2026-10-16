package src

import (
	"sync/atomic"
	"testing"
)

func TestAuthenticationErrorFromMAXPayload(t *testing.T) {
	err := authenticationErrorFromPayload(map[string]interface{}{
		"error":            "login.token",
		"message":          "FAIL_LOGIN_TOKEN",
		"localizedMessage": "Ошибка входа. Пожалуйста, авторизируйтесь снова",
	})
	if !IsAuthenticationError(err) {
		t.Fatalf("expected authentication error, got %v", err)
	}
	authErr, ok := err.(*AuthenticationError)
	if !ok {
		t.Fatalf("expected AuthenticationError, got %T", err)
	}
	if authErr.Code != "login.token" {
		t.Fatalf("code %q, want login.token", authErr.Code)
	}
	if authErr.Message != "Ошибка входа. Пожалуйста, авторизируйтесь снова" {
		t.Fatalf("unexpected message %q", authErr.Message)
	}
}

func TestAuthenticationErrorResponseIsRoutedToRequest(t *testing.T) {
	connection := NewConnection(&Config{}, nil)
	if !connection.isResponse(WebSocketPayload{"cmd": 3}) {
		t.Fatal("cmd=3 must be routed to the waiting request")
	}
}

func TestAuthenticationFailureIsReportedOnceUntilReset(t *testing.T) {
	client := NewClient(&Config{})
	var calls atomic.Int32
	client.OnAuthenticationFailure(func(error) { calls.Add(1) })

	client.ReportAuthenticationFailure(NewAuthenticationError("login.token", "expired"))
	client.ReportAuthenticationFailure(NewAuthenticationError("login.token", "expired"))
	if calls.Load() != 1 {
		t.Fatalf("calls %d, want 1", calls.Load())
	}

	client.ResetAuthenticationFailure()
	client.ReportAuthenticationFailure(NewAuthenticationError("login.token", "expired"))
	if calls.Load() != 2 {
		t.Fatalf("calls %d after reset, want 2", calls.Load())
	}
}
