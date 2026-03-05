package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"empireai/internal/runtime"
)

func TestDispatchSystemDirectiveViaDashboard_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"event_id":"evt-123","ok":true}`))
	}))
	defer srv.Close()

	t.Setenv("EMPIREAI_DIRECTIVE_ENDPOINT", srv.URL)
	t.Setenv("EMPIREAI_API_KEY", "k")

	eventID, attempted, err := dispatchSystemDirectiveViaDashboard(context.Background(), targetAgent{ID: "empire-coordinator"}, "SaaS in Uruguay")
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if !attempted {
		t.Fatal("expected attempted=true")
	}
	if eventID != "evt-123" {
		t.Fatalf("unexpected event id: %s", eventID)
	}
}

func TestDispatchSystemDirectiveViaDashboard_StatusErrorNonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	t.Setenv("EMPIREAI_DIRECTIVE_ENDPOINT", srv.URL)
	t.Setenv("EMPIREAI_API_KEY", "k")

	_, attempted, err := dispatchSystemDirectiveViaDashboard(context.Background(), targetAgent{ID: "empire-coordinator"}, "x")
	if !attempted {
		t.Fatal("expected attempted=true")
	}
	if err == nil {
		t.Fatal("expected error on non-2xx")
	}
	if got := err.Error(); got == "" || got == "bad request" {
		t.Fatalf("expected wrapped status error, got: %v", err)
	}
}

func TestDispatchSystemDirectiveViaDashboard_TransportErrorRetryable(t *testing.T) {
	t.Setenv("EMPIREAI_DIRECTIVE_ENDPOINT", "http://127.0.0.1:1")
	_, attempted, err := dispatchSystemDirectiveViaDashboard(context.Background(), targetAgent{ID: "empire-coordinator"}, "x")
	if !attempted {
		t.Fatal("expected attempted=true")
	}
	if err == nil {
		t.Fatal("expected transport error")
	}
	if got := err.Error(); got == "" {
		t.Fatalf("expected non-empty error, got: %v", err)
	}
}

func TestDispatchSystemDirective_FallbackToEventStore(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	t.Setenv("EMPIREAI_DIRECTIVE_ENDPOINT", srv.URL)
	t.Setenv("EMPIREAI_API_KEY", "k")

	eventID, err := dispatchSystemDirective(
		context.Background(),
		storeBundle{EventStore: runtime.InMemoryEventStore{}},
		targetAgent{ID: "empire-coordinator"},
		"SaaS in Argentina",
	)
	if err != nil {
		t.Fatalf("expected fallback success, got err=%v", err)
	}
	if strings.TrimSpace(eventID) == "" {
		t.Fatal("expected non-empty event id")
	}
}

func TestDispatchSystemDirective_NoEventStoreAndDashboardDownErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	t.Setenv("EMPIREAI_DIRECTIVE_ENDPOINT", srv.URL)
	t.Setenv("EMPIREAI_API_KEY", "k")

	_, err := dispatchSystemDirective(
		context.Background(),
		storeBundle{},
		targetAgent{ID: "empire-coordinator"},
		"SaaS in Argentina",
	)
	if err == nil {
		t.Fatal("expected directive dispatch error")
	}
	if !strings.Contains(err.Error(), "requires runtime interceptor") {
		t.Fatalf("expected interceptor path error, got: %v", err)
	}
}
