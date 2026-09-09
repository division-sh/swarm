package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

func TestServedResetRetainClearAndHistoricalReplayBothStores(t *testing.T) {
	scenario := servedparity.MustScenario(servedparity.ScenarioDestructiveResetLifecycle)
	servedparity.Run(t, scenario, func(t *testing.T, backend servedparity.Backend) {
		for _, clear := range []bool{false, true} {
			t.Run(fmt.Sprintf("clear=%t", clear), func(t *testing.T) {
				proof := startServedControlProofRuntime(t, backend)
				predecessorGrant, err := proof.Runtime.CurrentStartupGrantEvidence()
				if err != nil {
					t.Fatal(err)
				}
				var originalSource []byte
				if err := proof.DB.QueryRow("SELECT source_blob FROM source_artifacts WHERE bundle_hash = $1", proof.BundleHash).Scan(&originalSource); err != nil {
					t.Fatal(err)
				}
				initial := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
					"event_name": "item.received", "bundle_hash": proof.BundleHash,
					"payload": map[string]any{"item_id": "before-reset"}, "idempotency_key": uuid.NewString(),
				})
				waitForServedEventPublishNodeDeliveryLifecycle(t, proof.DB, proof.Backend, initial.RunID, initial.EventID, proof.Probe)
				params := map[string]any{"include_source_artifacts": clear, "idempotency_key": uuid.NewString()}
				lease, err := proof.Runtime.WorkOccurrence().Begin(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if lease != nil {
						_ = lease.Done()
					}
				})
				resetDone := make(chan servedJSONRPCEnvelope, 1)
				go func() { resetDone <- requestServedJSONRPC(t, proof.Endpoint, "runtime.nuke", params) }()
				deadline := time.After(10 * time.Second)
				for {
					probeLease, err := proof.Runtime.WorkOccurrence().Begin(context.Background())
					if errors.Is(err, worklifetime.ErrAdmissionFenced) || errors.Is(err, worklifetime.ErrRetired) {
						break
					}
					if err != nil {
						t.Fatalf("observe reset fence: %v", err)
					}
					_ = probeLease.Done()
					select {
					case <-deadline:
						t.Fatal("reset did not fence its predecessor")
					case <-time.After(time.Millisecond):
					}
				}
				select {
				case response := <-resetDone:
					t.Fatalf("reset passed active predecessor work: %+v", response)
				default:
				}
				var beforeJoin int
				if err := proof.DB.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", initial.RunID).Scan(&beforeJoin); err != nil || beforeJoin != 1 {
					t.Fatalf("cleanup preceded predecessor join: %d, %v", beforeJoin, err)
				}
				if err := lease.Done(); err != nil {
					t.Fatal(err)
				}
				lease = nil
				var first servedJSONRPCEnvelope
				select {
				case first = <-resetDone:
				case <-time.After(10 * time.Second):
					t.Fatal("reset did not finish after predecessor settlement")
				}
				if first.Error != nil {
					t.Fatalf("reset: %+v", first.Error)
				}
				var outcome map[string]any
				if err := json.Unmarshal(first.Result, &outcome); err != nil {
					t.Fatal(err)
				}
				var oldRuns, artifacts int
				if err := proof.DB.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", initial.RunID).Scan(&oldRuns); err != nil || oldRuns != 0 {
					t.Fatalf("predecessor run remains: %d, %v", oldRuns, err)
				}
				if err := proof.DB.QueryRow("SELECT COUNT(*) FROM source_artifacts WHERE bundle_hash = $1", proof.BundleHash).Scan(&artifacts); err != nil {
					t.Fatal(err)
				}
				if (artifacts == 0) != clear {
					t.Fatalf("source count=%d clear=%t", artifacts, clear)
				}
				var laterRun string
				if clear {
					response := requestServedJSONRPC(t, proof.Endpoint, "event.publish", map[string]any{
						"event_name": "item.received", "bundle_hash": proof.BundleHash,
						"payload": map[string]any{"item_id": "must-not-run"}, "idempotency_key": uuid.NewString(),
					})
					if response.Error == nil || response.Error.Data["code"] != apiv1.BundleUnavailableCode {
						t.Fatalf("cleared source execution error = %+v, want %s", response.Error, apiv1.BundleUnavailableCode)
					}
				} else {
					var retainedSource []byte
					if err := proof.DB.QueryRow("SELECT source_blob FROM source_artifacts WHERE bundle_hash = $1", proof.BundleHash).Scan(&retainedSource); err != nil || !bytes.Equal(originalSource, retainedSource) {
						t.Fatalf("retained admitted source changed: %v", err)
					}
					use, _, err := proof.Contexts.AcquireBundleHash(context.Background(), proof.BundleHash)
					if err != nil || use == nil {
						t.Fatalf("successor unavailable: %v", err)
					}
					successor := use.Runtime()
					grant, grantErr := successor.CurrentStartupGrantEvidence()
					if err := use.Done(); err != nil {
						t.Fatal(err)
					}
					if grantErr != nil || successor == proof.Runtime || grant.GrantID == predecessorGrant.GrantID {
						t.Fatalf("reset did not create fresh execution authority: %v", grantErr)
					}
					later := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
						"event_name": "item.received", "bundle_hash": proof.BundleHash,
						"payload": map[string]any{"item_id": "after-reset"}, "idempotency_key": uuid.NewString(),
					})
					waitForServedEventPublishNodeDeliveryLifecycle(t, proof.DB, proof.Backend, later.RunID, later.EventID, proof.Probe)
					requireServedParitySettlementPostconditions(t, proof.Endpoint, proof.DB, proof.Backend, later.RunID, scenario)
					laterRun = later.RunID
				}
				// The domain receipt must survive transport-cache expiry even
				// after later real runtime work has completed.
				expireServedResetTransportCache(t, proof)
				for i := 0; i < 2; i++ {
					replay := requestServedJSONRPC(t, proof.Endpoint, "runtime.nuke", params)
					if replay.Error != nil {
						t.Fatalf("historical replay: %+v", replay.Error)
					}
					var replayed map[string]any
					if err := json.Unmarshal(replay.Result, &replayed); err != nil || !reflect.DeepEqual(outcome, replayed) {
						t.Fatalf("outcome changed: %v\n%s\n%s", err, first.Result, replay.Result)
					}
					if laterRun != "" {
						var count int
						if err := proof.DB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM runs WHERE run_id = $1", laterRun).Scan(&count); err != nil || count != 1 {
							t.Fatalf("replay changed later run: %d, %v", count, err)
						}
					}
				}
				var phase string
				if err := proof.DB.QueryRow("SELECT phase FROM runtime_reset_operations").Scan(&phase); err != nil || phase != string(destructivereset.PhaseCompleted) {
					t.Fatalf("reset phase=%s, %v", phase, err)
				}
				if clear {
					// Cleared execution has no run to diagnose; its persisted
					// work must be absent rather than silently orphaned.
					for _, table := range []string{"event_deliveries", "events", "timers"} {
						var count int
						if err := proof.DB.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE run_id = $1", initial.RunID).Scan(&count); err != nil || count != 0 {
							t.Fatalf("cleared reset retained %s: count=%d err=%v", table, count, err)
						}
					}
				}
			})
		}
	})
}

func TestServedResetCLIDryRunApplyAndReplayBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			proof := startServedControlProofRuntime(t, backend)
			initial := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": proof.BundleHash,
				"payload": map[string]any{"item_id": "cli-reset"}, "idempotency_key": uuid.NewString(),
			})
			waitForServedEventPublishNodeDeliveryLifecycle(t, proof.DB, proof.Backend, initial.RunID, initial.EventID, proof.Probe)
			stdout, stderr, code := runServedCLICommand(t, proof.Endpoint, []string{"control", "nuke", "--dry-run"})
			if code != 0 || stderr != "" || !strings.Contains(stdout, "dry_run") {
				t.Fatalf("dry run: exit=%d stderr=%q stdout=%q", code, stderr, stdout)
			}
			for _, table := range []string{"runs", "source_artifacts"} {
				var count int
				if err := proof.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count == 0 {
					t.Fatalf("dry run changed %s: count=%d err=%v", table, count, err)
				}
			}
			var operations int
			if err := proof.DB.QueryRow("SELECT COUNT(*) FROM runtime_reset_operations").Scan(&operations); err != nil || operations != 0 {
				t.Fatalf("dry run admitted destructive operation: count=%d err=%v", operations, err)
			}
			key := uuid.NewString()
			args := []string{"control", "nuke", "--yes", "--idempotency-key", key}
			stdout, stderr, code = runServedCLICommand(t, proof.Endpoint, args)
			if code != 0 || stderr != "" || !strings.Contains(stdout, "completed") {
				t.Fatalf("apply: exit=%d stderr=%q stdout=%q", code, stderr, stdout)
			}
			for _, table := range []string{"runs", "source_artifacts"} {
				var count int
				if err := proof.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("apply retained %s: count=%d err=%v", table, count, err)
				}
			}
			expireServedResetTransportCache(t, proof)
			replayed, stderr, code := runServedCLICommand(t, proof.Endpoint, args)
			if code != 0 || stderr != "" || replayed != stdout {
				t.Fatalf("replay: exit=%d stderr=%q\nfirst=%s\nreplay=%s", code, stderr, stdout, replayed)
			}
			if err := proof.DB.QueryRow("SELECT COUNT(*) FROM runtime_reset_operations").Scan(&operations); err != nil || operations != 1 {
				t.Fatalf("CLI replay minted another operation: count=%d err=%v", operations, err)
			}
		})
	}
}

func expireServedResetTransportCache(t *testing.T, proof servedControlProofRuntime) {
	t.Helper()
	var owner apiv1.APIIdempotencyStore = proof.Postgres
	if proof.Backend == "sqlite" {
		owner = proof.SQLite
	}
	// Exercise the cache owner's expiry transaction, rather than racing a raw
	// fixture DELETE against the selected runtime's SQLite write owner.
	_, _, err := owner.WithAPIIdempotency(context.Background(), apiidempotency.Request{
		Method: "test.expire-transport-cache", ActorTokenID: "expiry-proof",
		IdempotencyKey: uuid.NewString(), RequestHash: "expiry-proof",
		Now: time.Now().UTC().Add(48 * time.Hour), TTL: time.Minute,
	}, func(context.Context) (apiidempotency.Completion, error) {
		return apiidempotency.Completion{Response: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var cached int
	if err := proof.DB.QueryRow("SELECT COUNT(*) FROM api_idempotency WHERE method = 'runtime.nuke'").Scan(&cached); err != nil || cached != 0 {
		t.Fatalf("reset transport cache was not expired: count=%d err=%v", cached, err)
	}
}
