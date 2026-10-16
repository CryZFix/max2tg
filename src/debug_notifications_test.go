package src

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSendDebugMessageReturnsTelegramAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"ok":false,"description":"bot was blocked by the user"}`))
	}))
	defer server.Close()

	sender := &TelegramSender{
		botToken:   "TOKEN",
		config:     &Config{TelegramAPIURL: server.URL},
		httpClient: server.Client(),
	}
	err := sender.SendDebugMessage("test", 123)
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("expected HTTP 403 error, got %v", err)
	}
}
