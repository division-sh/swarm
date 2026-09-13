package serveapp

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/servedparity"
)

// Compile-only injection keeps the real producers, plans and transaction owners
// intact without adding a production hook or a replaceable response projector.
func TestMailboxResponseProjectionRollbackBothStores(t *testing.T) {
	if os.Getenv("SWARM_MAILBOX_PROJECTION_OVERLAY") == "1" {
		runMailboxResponseProjectionRollback(t)
		return
	}
	root := repoRootForTest()
	dir := t.TempDir()
	replacements := map[string]string{}
	for _, site := range []struct{ path, anchor, identity string }{
		{"internal/runtime/pipeline/decision_card_mutation.go", "\traw, err := canonicaljson.Bytes(result)\n", "cardID"},
		{"internal/store/internal/mailboxpersistence/acknowledgment.go", "\traw, err := canonicaljson.Bytes(map[string]any{\"ok\": true, \"mailbox_id\": id, \"kind\": decisioncard.KindNotice})\n", "id"},
	} {
		path := filepath.Join(root, site.path)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		source := string(raw)
		if strings.Count(source, site.anchor) != 1 || strings.Count(source, "import (\n") != 1 {
			t.Fatalf("projection injection no longer uniquely identifies %s", site.path)
		}
		injection := "\tif os.Getenv(\"SWARM_MAILBOX_PROJECTION_RESOURCE\") == " + site.identity + ` {
        if err := os.WriteFile(os.Getenv("SWARM_MAILBOX_PROJECTION_WITNESS"), []byte(` + site.identity + `), 0600); err != nil {
            return apiidempotency.Completion{}, err
        }
        return apiidempotency.Completion{}, fmt.Errorf("mailbox_exact_response_projection_cut")
    }
`
		source = strings.Replace(source, "import (\n", "import (\n\t\"os\"\n", 1)
		source = strings.Replace(source, site.anchor, injection+site.anchor, 1)
		replacement := filepath.Join(dir, filepath.Base(path))
		if err := os.WriteFile(replacement, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		replacements[path] = replacement
	}
	raw, err := json.Marshal(map[string]any{"Replace": replacements})
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlay, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-overlay", overlay, "./internal/serveapp", "-run", "^TestMailboxResponseProjectionRollbackBothStores$", "-count=1", "-timeout=3m", "-v")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "SWARM_MAILBOX_PROJECTION_OVERLAY=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("real-transaction projection fault proof: %v\n%s", err, output)
	}
	t.Logf("compiler-overlay proof (not a race-instrumented child):\n%s", output)
}

func runMailboxResponseProjectionRollback(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt, owner := newMailboxCompletionRuntime(t, backend)
			for _, kind := range []decisioncard.AnchorKind{decisioncard.AnchorKindStageGate, decisioncard.AnchorKindHumanTask, decisioncard.AnchorKindProposedEffect, "notice"} {
				methods := []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input"}
				if kind == "notice" {
					methods = []string{"mailbox.acknowledge"}
				}
				for _, method := range methods {
					t.Run(string(kind)+"/"+method, func(t *testing.T) {
						f := mailboxCompletionFixtureInRuntime(t, rt, owner)
						params := mailboxPrincipalMutationParams(t, f, kind, method)
						req := mailboxPrincipalRequest(t, rt, method, params)
						before := mailboxCompletionRunEffects(t, rt, f.base.RunID)
						witness := filepath.Join(t.TempDir(), "projection-reached")
						t.Setenv("SWARM_MAILBOX_PROJECTION_WITNESS", witness)
						t.Setenv("SWARM_MAILBOX_PROJECTION_RESOURCE", req.ResourceID)
						_, replayed, err := mailboxTokenlessMutation(f.ctx, rt, owner, req, params)
						if err == nil || !strings.Contains(err.Error(), "mailbox_exact_response_projection_cut") || replayed {
							t.Fatalf("exact response projection did not fail: replay=%t err=%v", replayed, err)
						}
						seen, err := os.ReadFile(witness)
						if err != nil || string(seen) != req.ResourceID {
							t.Fatalf("wrong projection fault cut: %s err=%v", seen, err)
						}
						if after := mailboxCompletionRunEffects(t, rt, f.base.RunID); !reflect.DeepEqual(before, after) {
							t.Fatalf("projection error committed domain: %s", mailboxEffectsDifference(before, after))
						}
						var count int
						if err := rt.DB.QueryRow(`SELECT count(*) FROM api_idempotency WHERE idempotency_key=$1`, req.IdempotencyKey).Scan(&count); err != nil || count != 0 {
							t.Fatalf("projection failure committed completion: count=%d err=%v", count, err)
						}
						if kind == "notice" {
							var notified bool
							if err := rt.DB.QueryRow(`SELECT notified FROM mailbox WHERE item_id=$1`, req.ResourceID).Scan(&notified); err != nil || notified {
								t.Fatalf("projection failure acknowledged notice: notified=%t err=%v", notified, err)
							}
						}
						t.Setenv("SWARM_MAILBOX_PROJECTION_RESOURCE", "")
						var result map[string]any
						requireServedJSONRPCResult(t, rt.Endpoint, method, params, &result)
						if result["ok"] != true || result["idempotency_replayed"] != false {
							t.Fatalf("projection repair control did not commit once: %v", result)
						}
					})
				}
			}
		})
	}
}
