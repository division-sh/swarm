package serveapp

import (
	"encoding/base64"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedMailboxCursorSourceRoundTrip(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt, _, _, _, want := startServedMailboxEntityFilterProof(t, backend)
			for _, anchor := range []string{"", "stage_gate"} {
				t.Run("anchor="+anchor, func(t *testing.T) {
					params := map[string]any{"limit": 1, "anchor_kind": anchor}
					seen := map[string]bool{}
					pages := 0
					for {
						var page servedEntityFilterList
						requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", params, &page)
						pages++
						if len(page.Items) != 1 || pages > 4 {
							t.Fatalf("page %d: %+v", pages, page)
						}
						if id := page.Items[0].Card.CardID; id != "" {
							if seen[id] || !want[id] {
								t.Fatalf("duplicate/unexpected card %q", id)
							}
							seen[id] = true
						}
						if page.NextCursor == "" {
							break
						}
						params["cursor"] = page.NextCursor
					}
					if len(seen) != len(want) || pages < 3 {
						t.Fatalf("got %v in %d pages, want %v", seen, pages, want)
					}
				})
			}
			t.Run("suppressed_owner_admission", func(t *testing.T) {
				cursor := base64.RawURLEncoding.EncodeToString([]byte(`{"notice":"not-base64!"}`))
				err := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.list", map[string]any{"anchor_kind": "stage_gate", "cursor": cursor})
				if err.Code != -32602 {
					t.Fatalf("invalid supplied owner cursor: %+v", err)
				}
			})
		})
	}
}
