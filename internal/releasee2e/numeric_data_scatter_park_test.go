package releasee2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const numericScatterSource = "tests/tier11-flow-composition/test-numeric-data-scatter-park"
const goldenShutdownGrace = 30 * time.Second

// Establish the approved observable interruption boundary before giving the
// larger corpus any recovery credit. No private runtime hooks stop the pump.
func TestGoldenNumericDataScatterParkRestartBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for both-store numeric recovery proof", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	lifecycle := buildOwnedMockLifecycleBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			store := goldenSQLiteStore(root)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			writeReleaseFile(t, filepath.Join(root, "go.mod"), "module numeric-hostile-ancestor\n\ngo 1.23.0\n")
			project := filepath.Join(root, "yaml-project")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), numericScatterSource), filepath.Join(project, "contracts"))
			expected, corpus := loadNumericFeedCorpus(t, filepath.Join(project, "contracts"))
			writeReleaseFile(t, filepath.Join(project, ".swarm/swarm.yaml"), goldenRuntimeConfig(store))
			writeReleaseFile(t, filepath.Join(project, "api-token"), goldenAPIToken+"\n")
			env := goldenProcessEnv(t, root, store.passwordEnv, 0)
			assertGoldenProcessHasNoExternalExecutables(t, env)
			writeReleaseFile(t, filepath.Join(root, "home/.config/swarm/swarm.yaml"), "connection:\n  api_token_file: api-token\n")
			cliEnv := make([]string, 0, len(env))
			for _, entry := range env {
				if !strings.HasPrefix(entry, goldenPostgresPass+"=") {
					cliEnv = append(cliEnv, entry)
				}
			}
			verified := runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary,
				"verify", "contracts", "--config", ".swarm/swarm.yaml", "--json")
			if verified.err != nil {
				t.Fatalf("numeric verify: %v\n%s", verified.err, verified.output)
			}
			start := func() *releaseServeProcess {
				p := startReleaseServe(t, releaseProcessSpec{
					BinaryPath: binary, InternalMockLifecycleBinary: lifecycle,
					WorkingDir: project, Source: "contracts", ConfigPath: ".swarm/swarm.yaml",
					Store: backend, TokenFile: "api-token", Token: goldenAPIToken, Env: env,
					ShutdownGrace: goldenShutdownGrace,
				})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := p.waitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := start()
			creationEndpoint := p.apiBase
			runID := uuid.NewString()
			issued := time.Now()
			goldenServedBundleHash(t, p.rpc, "mock_only")
			create := func() releaseCommandResult {
				return runReleaseCommand(t, goldenStartupTimeout, project, cliEnv, "", binary,
					"run", "start", "--connect", p.apiBase,
					"--run-id", runID, "--idempotency-key", "numeric-"+runID,
					"--data", "item.registered=contracts/data/items.jsonl", "--no-follow")
			}
			result := create()
			if result.err != nil || !strings.Contains(result.output, "run_id="+runID) {
				t.Fatalf("numeric compiled run start: %v\n%s", result.err, result.output)
			}
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				feed, err := readNumericFeed(ctx, p.rpc, runID)
				if err != nil {
					return false, err
				}
				if feed.Cursor == feed.Cardinality {
					return false, fmt.Errorf("feed finished before a partial checkpoint was observable: %+v", feed)
				}
				return feed.Cursor > 0, nil
			}); err != nil {
				t.Fatalf("public partial-work checkpoint: %v\nprocess evidence:\n%s", err, p.output.String())
			}
			pauseFullLifecycleRun(t, p.rpc, runID)
			checkpoint, err := readNumericFeed(ctx, p.rpc, runID)
			if err != nil {
				t.Fatalf("public numeric checkpoint: %v\nprocess evidence:\n%s", err, p.output.String())
			}
			if checkpoint.Runtime.Availability != "available" || checkpoint.Runtime.Eligible == nil || *checkpoint.Runtime.Eligible || checkpoint.Runtime.Reason != "run_paused" {
				t.Fatalf("runtime-enriched paused readback lost exact admission: %+v", checkpoint.Runtime)
			}
			listed := runReleaseCommand(t, goldenStartupTimeout, project, cliEnv, "", binary,
				"run", "fan-out", "list", runID, "--api-server", p.apiBase, "--api-token-file", "api-token", "--limit", "1", "--json")
			if listed.err != nil {
				t.Fatalf("compiled fan-out reader: %v\n%s", listed.err, listed.output)
			}
			cliFeed, err := decodeNumericFeedPage([]byte(strings.TrimSpace(listed.output)), runID, "paused")
			if err != nil || cliFeed.Key != checkpoint.Key || cliFeed.BundleHash != checkpoint.BundleHash {
				t.Fatalf("compiled CLI changed deployment readback: %+v, %v", cliFeed, err)
			}
			predecessor, err := listGoldenEvents(ctx, p.rpc, runID)
			if err != nil {
				t.Fatal(err)
			}
			unsettled := 0
			for _, event := range predecessor {
				for _, delivery := range event.Deliveries {
					if !delivery.Terminal {
						unsettled++
					}
				}
			}
			t.Logf("public pre-kill checkpoint after %s: status=%s feed=%s cursor=%d/%d owed=%d events=%d unsettled=%d", time.Since(issued), "paused", checkpoint.Status, checkpoint.Cursor, checkpoint.Cardinality, checkpoint.Owed, len(predecessor), unsettled)
			if checkpoint.Cardinality != 100 || checkpoint.Status != "open" || checkpoint.Cursor == 0 || checkpoint.Cursor >= 100 || checkpoint.Owed == 0 || unsettled == 0 {
				t.Fatalf("approved interrupted checkpoint is unreachable: %#v; no settled-restart credit", checkpoint)
			}
			beforeRestart := captureFullLifecycleEvidence(t, p.rpc, runID)
			if err := p.killAndWait(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			p = start()
			retained, err := readNumericFeed(ctx, p.rpc, runID)
			if err != nil || retained.Cursor != checkpoint.Cursor || retained.Owed != checkpoint.Owed || retained.Status != checkpoint.Status {
				t.Fatalf("paused restart changed feed: before=%+v after=%+v error=%v\n%s", checkpoint, retained, err, p.output.String())
			}
			waitForFullLifecycleRunStatus(t, p.rpc, runID, "paused")
			afterRestart := captureFullLifecycleEvidence(t, p.rpc, runID)
			for eventID, before := range beforeRestart.EventFacts {
				if after := afterRestart.EventFacts[eventID]; after != before {
					t.Fatalf("paused restart changed event/delivery %s: before=%s after=%s", eventID, before, after)
				}
			}
			for eventID, encoded := range afterRestart.EventFacts {
				if _, existed := beforeRestart.EventFacts[eventID]; existed {
					continue
				}
				var row fullLifecycleEvent
				if err := json.Unmarshal([]byte(encoded), &row); err != nil {
					t.Fatal(err)
				}
				// Active topology can project lifecycle observations while dispatch
				// is parked. Only its canonical non-delivery subtype is exempt.
				if row.EventName != "platform.runtime_log" || len(row.Deliveries) != 0 || len(row.DeadLetters) != 0 || row.NoDelivery == nil || row.NoDelivery.Reason != "no_subscriber_by_design" {
					t.Fatalf("paused restart produced executable event %s: %s", eventID, encoded)
				}
			}
			var continued any
			if err := p.rpc.call(ctx, "run.continue", map[string]any{"run_id": runID, "idempotency_key": "numeric-continue-" + runID}, &continued); err != nil {
				t.Fatal(err)
			}
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				feed, err := readNumericFeed(ctx, p.rpc, runID)
				if err != nil {
					return false, err
				}
				return feed.Status == "closed" && feed.Cursor == 100 && feed.Owed == 0, nil
			}); err != nil {
				t.Fatal(err)
			}
			final, err := listGoldenEvents(ctx, p.rpc, runID)
			if err != nil {
				t.Fatal(err)
			}
			if countGoldenEvents(final, "item.registered") != 100 {
				t.Fatalf("recovered row count = %d, want 100", countGoldenEvents(final, "item.registered"))
			}
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				rows, err := listGoldenEvents(ctx, p.rpc, runID)
				if err != nil {
					return false, err
				}
				completed := 0
				for _, row := range rows {
					if row.EventName != "item.registered" {
						continue
					}
					if len(row.Deliveries) != 1 || row.Deliveries[0].SubscriberType != "node" || len(row.DeadLetters) != 0 {
						return false, fmt.Errorf("wrong intake recipient for %s: %+v", row.EventID, row)
					}
					delivery := row.Deliveries[0]
					if delivery.Status == "dead_letter" {
						return false, fmt.Errorf("intake dead letter: %+v", delivery)
					}
					if delivery.Status == "delivered" && delivery.Terminal {
						completed++
					}
				}
				return completed == 100, nil
			}); err != nil {
				t.Fatalf("numeric intake convergence: %v\n%s", err, p.output.String())
			}
			entities := assertNumericFeedPublic(t, ctx, p.rpc, runID, expected, corpus)
			creation := readNumericCreationReceipt(t, ctx, p.rpc, runID)
			settled := captureFullLifecycleEvidence(t, p.rpc, runID)
			closed, err := readNumericFeed(ctx, p.rpc, runID)
			if err != nil {
				t.Fatal(err)
			}
			shutdownStarted := time.Now()
			if err := p.stopAndWait(goldenShutdownGrace + 5*time.Second); err != nil {
				t.Fatalf("numeric shutdown: %v\n%s", err, p.output.String())
			}
			t.Logf("100-item shutdown took %s with documented default grace %s", time.Since(shutdownStarted), goldenShutdownGrace)
			timers := inspectNumericFeedTimers(t, ctx, lifecycle, project, env, runID, expected, entities)
			p = start()
			if got := assertNumericFeedPublic(t, ctx, p.rpc, runID, expected, corpus); !reflect.DeepEqual(got, entities) {
				t.Fatal("settled restart changed exact receiver identities")
			}
			restored := captureFullLifecycleEvidence(t, p.rpc, runID)
			for eventID, facts := range settled.EventFacts {
				if got := restored.EventFacts[eventID]; got != facts {
					t.Fatalf("settled restart changed event/delivery %s", eventID)
				}
			}
			replayed := create()
			wantOutput := strings.ReplaceAll(result.output, creationEndpoint, p.apiBase)
			if replayed.err != nil || replayed.output != wantOutput {
				t.Fatalf("permanent creation retry after process loss: err=%v before=%s after=%s", replayed.err, result.output, replayed.output)
			}
			if got := readNumericCreationReceipt(t, ctx, p.rpc, runID); !reflect.DeepEqual(got, creation) {
				t.Fatal("permanent creation receipt changed on retry")
			}
			if got := assertNumericFeedPublic(t, ctx, p.rpc, runID, expected, corpus); !reflect.DeepEqual(got, entities) {
				t.Fatal("permanent retry recreated or retargeted receivers")
			}
			afterReplay := captureFullLifecycleEvidence(t, p.rpc, runID)
			if !reflect.DeepEqual(afterReplay, restored) {
				t.Fatal("permanent retry changed frozen run/domain facts")
			}
			if got, err := readNumericFeed(ctx, p.rpc, runID); err != nil || got.Key != closed.Key || got.Cursor != closed.Cursor || got.Cardinality != closed.Cardinality || got.Status != closed.Status {
				t.Fatalf("permanent retry changed exact feed: %+v %v", got, err)
			}
			if err := p.stopAndWait(goldenShutdownGrace + 5*time.Second); err != nil {
				t.Fatalf("numeric retained shutdown: %v\n%s", err, p.output.String())
			}
			if got := inspectNumericFeedTimers(t, ctx, lifecycle, project, env, runID, expected, entities); !reflect.DeepEqual(got, timers) {
				t.Fatal("settled restart/retry changed exact typed timer activations")
			}
		})
	}
}

// Independent public wire projections deliberately do not reuse the server DTO
// or its validator: doing so can make the oracle repeat the implementation bug.
type numericFeedReadback struct {
	Key struct {
		RunID  string `json:"run_id"`
		FeedID string `json:"deployment_feed_id"`
	} `json:"key"`
	BundleHash  string `json:"bundle_hash"`
	Status      string `json:"status"`
	Cardinality int    `json:"cardinality"`
	Cursor      int    `json:"cursor"`
	Owed        int    `json:"owed"`
	Runtime     struct {
		Availability string `json:"availability"`
		Eligible     *bool  `json:"eligible"`
		Reason       string `json:"reason"`
	} `json:"runtime"`
}

func decodeNumericFeedPage(body []byte, runID, status string) (numericFeedReadback, error) {
	var page struct {
		RunID      string            `json:"run_id"`
		RunStatus  string            `json:"run_status"`
		ObservedAt time.Time         `json:"observed_at"`
		Order      string            `json:"order"`
		Intents    []json.RawMessage `json:"intents"`
		NextCursor string            `json:"next_cursor"`
	}
	var feed numericFeedReadback
	if err := json.Unmarshal(body, &page); err != nil {
		return feed, err
	}
	if page.RunID != runID || page.ObservedAt.IsZero() || page.Order != "intent_identity_asc" || len(page.Intents) != 1 || page.NextCursor != "" || (status != "" && page.RunStatus != status) {
		return feed, fmt.Errorf("numeric feed requires one exact public intent page: %s", body)
	}
	if page.RunStatus != "running" && page.RunStatus != "paused" {
		return feed, fmt.Errorf("numeric active-run readback changed status: %s", page.RunStatus)
	}
	if err := json.Unmarshal(page.Intents[0], &feed); err != nil {
		return feed, err
	}
	var raw struct {
		Key map[string]json.RawMessage `json:"key"`
	}
	if err := json.Unmarshal(page.Intents[0], &raw); err != nil {
		return feed, err
	}
	if len(raw.Key) != 2 || feed.Key.RunID != runID || feed.Key.FeedID == "" || feed.BundleHash == "" || feed.Cardinality != 100 || feed.Cursor < 0 || feed.Cursor > 100 || feed.Owed < 0 || feed.Owed > 100-feed.Cursor {
		return feed, fmt.Errorf("numeric deployment origin/shape changed: %s", page.Intents[0])
	}
	if _, err := uuid.Parse(feed.Key.FeedID); err != nil || (feed.Status != "open" && feed.Status != "closed") {
		return feed, fmt.Errorf("numeric feed identity/status changed: %s", page.Intents[0])
	}
	return feed, nil
}

func TestNumericFeedReadbackRejectsWrongOriginAndPage(t *testing.T) {
	runID, feedID := uuid.NewString(), uuid.NewString()
	valid := fmt.Sprintf(`{"run_id":%q,"run_status":"paused","observed_at":"2026-09-30T00:00:00Z","order":"intent_identity_asc","intents":[{"key":{"run_id":%q,"deployment_feed_id":%q},"bundle_hash":"sha256:numeric","status":"open","cardinality":100,"cursor":32,"owed":68}],"next_cursor":""}`, runID, runID, feedID)
	if _, err := decodeNumericFeedPage([]byte(valid), runID, "paused"); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"handler_leak":  strings.Replace(valid, `"deployment_feed_id":`, `"element_ref":{},"deployment_feed_id":`, 1),
		"missing_feed":  strings.Replace(valid, feedID, "", 1),
		"wrong_status":  strings.Replace(valid, `"paused"`, `"running"`, 1),
		"wrong_order":   strings.Replace(valid, "intent_identity_asc", "insertion_order", 1),
		"wrong_count":   strings.Replace(valid, `"cardinality":100`, `"cardinality":99`, 1),
		"invented_debt": strings.Replace(valid, `"owed":68`, `"owed":69`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeNumericFeedPage([]byte(body), runID, "paused"); err == nil {
				t.Fatal("invalid public numeric projection accepted")
			}
		})
	}
}

func readNumericFeed(ctx context.Context, rpc *releaseRPCClient, runID string) (numericFeedReadback, error) {
	var body json.RawMessage
	if err := rpc.call(ctx, "run.fan_out.list", map[string]any{"run_id": runID, "limit": 1}, &body); err != nil {
		return numericFeedReadback{}, err
	}
	return decodeNumericFeedPage(body, runID, "")
}
