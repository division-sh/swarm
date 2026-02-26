package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"empireai/internal/models"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestRuntimeToolExecutor_ExternalProxy_LoadsCredsDecryptsAndCallsEndpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Endpoint that asserts headers + method and returns JSON.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Fatalf("expected Authorization Bearer tok, got %q", got)
		}
		if got := r.Header.Get("X-From"); got != "cred" {
			t.Fatalf("expected X-From=cred, got %q", got)
		}
		if got := r.Header.Get("X-User"); got != "u" {
			t.Fatalf("expected X-User=u, got %q", got)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	oldKey := os.Getenv("EMPIREAI_CREDENTIALS_KEY")
	t.Cleanup(func() { _ = os.Setenv("EMPIREAI_CREDENTIALS_KEY", oldKey) })
	_ = os.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")

	verticalID := "11111111-1111-1111-1111-111111111111"

	// loadVerticalCredentials query + decrypt query.
	credsJSON, _ := json.Marshal(map[string]any{
		"whatsapp": map[string]any{
			"endpoint": srv.URL,
			"api_key":  "enc::dG9r", // base64("tok")
			"headers":  map[string]any{"X-From": "cred"},
		},
	})
	mock.ExpectQuery("SELECT COALESCE\\(credentials, '\\{\\}'::jsonb\\)\\s+FROM verticals").
		WithArgs(verticalID).
		WillReturnRows(sqlmock.NewRows([]string{"credentials"}).AddRow(credsJSON))
	mock.ExpectQuery("SELECT pgp_sym_decrypt").
		WithArgs("dG9r", "k").
		WillReturnRows(sqlmock.NewRows([]string{"plain"}).AddRow("tok"))

	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	exec.SetSQLDB(db)

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "a1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: verticalID,
	})

	out, err := exec.Execute(ctx, "whatsapp_business_api", map[string]any{
		"headers": map[string]any{"X-User": "u"},
		"body":    map[string]any{"hello": "world"},
	})
	if err != nil {
		t.Fatalf("external proxy: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["status"] != "ok" {
		t.Fatalf("unexpected out: %#v", out)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestRuntimeToolExecutor_ExternalProxy_DefaultMethodAndParseBody(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Endpoint that returns plain text.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	verticalID := "22222222-2222-2222-2222-222222222222"
	credsJSON, _ := json.Marshal(map[string]any{
		"registrar": map[string]any{
			"endpoint": srv.URL,
			"token":    "t1",
		},
	})
	mock.ExpectQuery("SELECT COALESCE\\(credentials, '\\{\\}'::jsonb\\)\\s+FROM verticals").
		WithArgs(verticalID).
		WillReturnRows(sqlmock.NewRows([]string{"credentials"}).AddRow(credsJSON))

	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	exec.SetSQLDB(db)

	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "opco-ceo", Mode: "operating", VerticalID: verticalID})

	out, err := exec.Execute(ctx, "domain_availability_check", map[string]any{
		// method empty -> defaultExternalMethod => GET
		"path":  "/v1/check",
		"query": map[string]any{"q": "x"},
	})
	if err != nil {
		t.Fatalf("domain_availability_check: %v", err)
	}
	m, _ := out.(map[string]any)
	body := m["body"]
	if s, ok := body.(string); !ok || strings.TrimSpace(s) != "ok" {
		t.Fatalf("expected plain body ok, got %#v", body)
	}

	// Small helpers.
	if defaultExternalMethod("whatsapp_name_check") != http.MethodGet {
		t.Fatal("expected GET for whatsapp_name_check")
	}
	if parseExternalResponseBody([]byte(`{"x":1}`) ) == nil {
		t.Fatal("expected parsed json")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
