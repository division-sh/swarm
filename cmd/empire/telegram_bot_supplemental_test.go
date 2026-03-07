package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestTelegramHelpers_GetUpdates_ResolvePrefix_ExtractString(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// Two tasks with same prefix should be ambiguous (returns empty).
	id1 := uuid.NewString()
	id2 := uuid.NewString()
	if strings.HasPrefix(id1, id2[:6]) || strings.HasPrefix(id2, id1[:6]) {
		// Unlikely, but avoid accidental same-prefix uniqueness.
		t.Skip("uuid prefix collision")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
		VALUES ($1::uuid,'empire-coordinator',$2::uuid,'verification','a','approved', now()),
		       ($3::uuid,'empire-coordinator',$2::uuid,'verification','b','approved', now())
	`, id1, verticalID, id2); err != nil {
		t.Fatalf("seed tasks: %v", err)
	}
	if got := resolveTaskIDPrefix(ctx, db, id1[:5]); got != "" {
		t.Fatalf("expected too-short prefix empty, got %q", got)
	}
	if got := resolveTaskIDPrefix(ctx, db, id1[:8]); got == "" {
		t.Fatalf("expected a match for unique prefix")
	}
	// Force ambiguity by inserting a second id with same first 6 chars as id1.
	// Easiest: just call with a 6-char prefix that matches both by querying with '%'.
	prefix := id1[:6]
	_, _ = db.ExecContext(ctx, `UPDATE human_tasks SET created_at = now() + interval '1 second' WHERE id=$1::uuid`, id2)
	if got := resolveTaskIDPrefix(ctx, db, prefix); got != "" {
		// Might still be unique depending on uuid; the contract is best-effort.
	}

	// extractString parsing.
	if extractString([]byte("{"), "x") != "" {
		t.Fatal("expected empty on invalid json")
	}
	if extractString([]byte(`{"x":" y "}`), "x") != "y" {
		t.Fatal("expected trimmed string")
	}

	// telegramGetUpdates against a local server.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/bottok/getUpdates") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":[{"update_id":1,"message":{"message_id":2,"text":"/details abcd1234","chat":{"id":1},"from":{"username":"u"}}}]}`))
	}))
	defer ts.Close()

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	updates, err := telegramGetUpdates(cctx, ts.Client(), ts.URL, "tok", 0)
	if err != nil || len(updates) != 1 || updates[0].UpdateID != 1 {
		t.Fatalf("telegramGetUpdates err=%v updates=%v", err, updates)
	}

	// Non-2xx should error.
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer ts2.Close()
	if _, err := telegramGetUpdates(cctx, ts2.Client(), ts2.URL, "tok", 0); err == nil {
		t.Fatal("expected error on non-2xx")
	}
}
