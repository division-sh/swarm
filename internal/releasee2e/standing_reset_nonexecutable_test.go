package releasee2e

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStandingResetNonExecutablePublicBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	lifecycle := buildOwnedMockLifecycleBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, product := range []string{"terminal", "terminal-revised", "terminal-suspended", "validated-invalid", "validated-invalid-revised", "orphan", "terminal-orphan", "invalid-orphan", "broken-relation"} {
				t.Run(product, func(t *testing.T) {
					root := filepath.Join(base, backend, product)
					store := goldenSQLiteStore(root)
					if backend == "postgres" {
						store = goldenPostgresStore(t, dsn)
					}
					if product == "orphan" || strings.HasSuffix(product, "-orphan") || product == "broken-relation" {
						runStandingOrphanRestorationPublic(t, binary, lifecycle, root, store, product)
						return
					}
					spec := prepareFullLifecycleProject(t, binary, root, store, false)
					spec.InternalMockLifecycleBinary = lifecycle
					writeReleaseFile(t, filepath.Join(spec.WorkingDir, ".swarm/swarm.yaml"), fullLifecycleRuntimeConfig(store, false))
					if err := os.Remove(filepath.Join(spec.WorkingDir, spec.ConfigPath)); err != nil {
						t.Fatal(err)
					}
					spec.ConfigPath = ".swarm/swarm.yaml"
					cliEnv := make([]string, 0, len(spec.Env))
					for _, entry := range spec.Env {
						if !strings.HasPrefix(entry, goldenPostgresPass+"=") {
							cliEnv = append(cliEnv, entry)
						}
					}
					start := func() *releaseServeProcess {
						p := startReleaseServe(t, spec)
						ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleStartupLimit)
						defer cancel()
						if err := p.waitReady(ctx); err != nil {
							t.Fatalf("non-executable startup: %v\n%s", err, p.output.String())
						}
						return p
					}
					p := start()
					bundle := requireFullLifecycleHealth(t, p.rpc)
					standing := waitForFullLifecycleStandingRun(t, p.rpc, bundle, "", 0, "")
					card := waitForFullLifecycleCard(t, p.rpc, standing.RunID, "lifecycle_ready")
					ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
					defer cancel()
					if strings.HasPrefix(product, "validated-invalid") {
						decideFullLifecycleCard(t, p.rpc, card, "invalid-precondition-approve")
					} else {
						var result any
						if err := p.rpc.call(ctx, "mailbox.decide", map[string]any{
							"card_id": card.CardID, "verdict": "reject", "fields": map[string]any{},
							"observed_content_hash": card.CardContentHash, "idempotency_key": "terminal-reject",
						}, &result); err != nil {
							t.Fatal(err)
						}
						waitForFullLifecycleRunStatus(t, p.rpc, standing.RunID, "completed")
					}
					before := captureFullLifecycleEvidence(t, p.rpc, standing.RunID)
					if err := p.stopAndWait(15 * time.Second); err != nil {
						t.Fatalf("precondition shutdown: %v\n%s", err, p.output.String())
					}
					if product == "terminal-suspended" || strings.HasPrefix(product, "validated-invalid") {
						// Explicit offline test precondition, not production repair:
						// preserve all identity relations and source facts while selecting
						// the canonical retained-override/invalid-current products.
						db := store.diagnosticDB
						if backend == "sqlite" {
							var err error
							db, err = sql.Open("sqlite", filepath.Join(root, "runtime.db"))
							if err != nil {
								t.Fatal(err)
							}
						}
						query := `UPDATE standing_services SET operator_override='suspended', effective_state='suspended', publication_state='pending', override_actor='test-precondition', override_at=? WHERE service_id=?`
						args := []any{time.Now().UTC(), standing.Origin.ServiceID}
						if backend == "postgres" {
							query = `UPDATE standing_services SET operator_override='suspended', effective_state='suspended', publication_state='pending', override_actor='test-precondition', override_at=$2 WHERE service_id=$1::uuid`
							args = []any{standing.Origin.ServiceID, time.Now().UTC()}
						}
						_, writeErr := db.ExecContext(ctx, query, args...)
						closeErr := db.Close()
						if writeErr != nil || closeErr != nil {
							t.Fatalf("offline precondition: write=%v close=%v", writeErr, closeErr)
						}
					}
					if strings.HasSuffix(product, "-revised") {
						prompt := filepath.Join(spec.WorkingDir, spec.Source, "telegram-chat/prompts/phrase-bot.md")
						body, err := os.ReadFile(prompt)
						if err != nil {
							t.Fatal(err)
						}
						writeReleaseFile(t, prompt, string(body)+"\nUse the admitted revised declaration.\n")
					}
					p = start()
					writeReleaseFile(t, filepath.Join(spec.WorkingDir, "operator.yaml"), "connection:\n  api_server: "+strconv.Quote(p.apiBase)+"\n  api_token_file: api-token\n")
					latestBundle := requireFullLifecycleHealth(t, p.rpc)
					if strings.HasSuffix(product, "-revised") && latestBundle == bundle {
						t.Fatal("cold revision did not change the admitted source")
					}
					if !reflect.DeepEqual(before, captureFullLifecycleEvidence(t, p.rpc, standing.RunID)) {
						t.Fatal("non-executable restart executed predecessor work")
					}
					for _, command := range []string{"suspend", "resume"} {
						result := runReleaseCommand(t, fullLifecycleStartupLimit, spec.WorkingDir, cliEnv, "", binary, "--config", "operator.yaml", "standing", command, standing.Origin.ServiceID, "--idempotency-key", "refuse-cli-"+command)
						if result.err == nil || !strings.Contains(result.output, "standing") {
							t.Fatalf("compiled %s did not refuse %s: err=%v output=%s", command, product, result.err, result.output)
						}
						var refused any
						if err := p.rpc.call(ctx, "standing."+command, map[string]any{"service_id": standing.Origin.ServiceID, "idempotency_key": "refuse-" + command}, &refused); err == nil {
							t.Fatalf("%s admitted non-executable %s", command, product)
						}
					}
					if !reflect.DeepEqual(before, captureFullLifecycleEvidence(t, p.rpc, standing.RunID)) {
						t.Fatal("refused non-executable commands changed predecessor facts")
					}
					var reset standingRuntimePublicResult
					if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": standing.Origin.ServiceID, "idempotency_key": "reset-non-executable"}, &reset); err != nil {
						t.Fatalf("%s public reset: %v\n%s", product, err, p.output.String())
					}
					wantState := "active"
					if product == "terminal-suspended" || strings.HasPrefix(product, "validated-invalid") {
						wantState = "suspended"
					}
					if reset.ServiceID != standing.Origin.ServiceID || reset.Generation != standing.Origin.Generation+1 || reset.RunID == standing.RunID || reset.EffectiveState != wantState {
						t.Fatalf("non-executable reset result=%+v", reset)
					}
					waitForFullLifecycleStandingRun(t, p.rpc, latestBundle, reset.ServiceID, reset.Generation, reset.RunID)
					if wantState == "active" {
						waitForFullLifecycleCard(t, p.rpc, reset.RunID, "lifecycle_ready")
					} else {
						waitForFullLifecycleRunStatus(t, p.rpc, reset.RunID, "paused")
					}
					if err := p.stopAndWait(15 * time.Second); err != nil {
						t.Fatalf("successor shutdown: %v\n%s", err, p.output.String())
					}
				})
			}
		})
	}
}

type standingRuntimePublicResult struct {
	ServiceID      string `json:"service_id"`
	RunID          string `json:"run_id"`
	Generation     int64  `json:"generation"`
	EffectiveState string `json:"effective_state"`
}
