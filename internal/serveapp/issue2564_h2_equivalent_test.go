package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Reconstructed equivalent H2, NOT the unchanged lead-local archive. Sources
// below reconstruct #2564's published root/keyed-child bundle under the explicit
// substitute permission in issuecomment-6001398406. Explicit timer IDs, current
// unquoted CEL slots, and this bounded-transport RPC/process driver are declared
// differences. Six hubs, 1s intervals, 900/40s^-1 and 600/13s^-1 submissions,
// and counter initialization are preserved; actual admission rates are separate.
// Deterministic old-head H2a/H2b/H2c mechanism witnesses remain separate proofs.
const issue2564H2ChildEnv = "SWARM_TEST_ISSUE2564_H2_CHILD"

const issue2564H2CorpusSHA = "14927d4bad9d6c7242a0f8112fe18d7d4fb25562d8f9147178320bd627518a54"

func TestIssue2564H2EquivalentCorpus(t *testing.T) {
	root := issue2564H2Source(t)
	t.Logf("reconstructed equivalent H2 admitted bundle=%s", servedEventPublishFixtureBundleHash(t, root))
}

type issue2564H2Child struct {
	Source, Config, Backend string
	Cut                     bool
}

type issue2564H2Cut struct {
	armed, captured atomic.Bool
	output          io.Writer
}

func (c *issue2564H2Cut) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.EventPersisted && signal.EventType == "hub.bump" {
		fmt.Fprintf(os.Stdout, "H2_DURABLE_ADMISSION event=%s observed_at=%s\n", signal.EventID, time.Now().UTC().Format(time.RFC3339Nano))
	}
	if signal.Kind != lifecycleprobe.EventPersisted || signal.EventType != "platform.stage_timer" || !c.armed.Load() || !c.captured.CompareAndSwap(false, true) {
		return
	}
	// This existing observer is after the real occurrence/publication commit and
	// before dispatch. The parent kills this process; there is no graceful release.
	if err := json.NewEncoder(c.output).Encode(signal); err != nil {
		panic(err)
	}
	<-ctx.Done()
}

func TestIssue2564H2ServeProcessHelper(t *testing.T) {
	raw := os.Getenv(issue2564H2ChildEnv)
	if raw == "" {
		t.Skip("parent-owned reconstructed equivalent H2 process")
	}
	var request issue2564H2Child
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath = request.Source, request.Config
	opts.StoreMode, opts.StoreModeSet = request.Backend, true
	opts.PlatformSpecPath = defaultPlatformSpecPath
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.SelfCheck, opts.Verbose, opts.Dev = true, true, false
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	if request.Cut {
		arm, reported := os.NewFile(3, "h2-arm"), os.NewFile(4, "h2-occurrence-committed")
		defer arm.Close()
		defer reported.Close()
		cut := &issue2564H2Cut{output: reported}
		opts.TestLifecycleProbe = cut
		go func() {
			for {
				var command [1]byte
				if _, err := io.ReadFull(arm, command[:]); err != nil {
					return
				}
				if command[0] == 1 {
					cut.armed.Store(true)
				} else if command[0] == 2 {
					stack := make([]byte, 4<<20)
					n := runtime.Stack(stack, true)
					fmt.Fprintf(os.Stderr, "H2_STACK_DUMP_BEGIN\n%s\nH2_STACK_DUMP_END\n", stack[:n])
				}
			}
		}()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("non-dev H2 serve exit=%d", code)
	}
}

func issue2564H2Source(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyIssue2564H2Equivalent(t)
	paths := []string{"schema.yaml", "events.yaml", "hub/schema.yaml", "hub/entities.yaml", "hub/nodes.yaml"}
	slices.Sort(paths)
	manifest := sha256.New()
	for _, path := range paths {
		contents, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		checksum := sha256.Sum256(contents)
		fmt.Fprintf(manifest, "%x  %s\n", checksum, path)
		t.Logf("reconstructed_equivalent_H2 source=%s sha256=%x", path, checksum)
	}
	checksum := fmt.Sprintf("%x", manifest.Sum(nil))
	if checksum != issue2564H2CorpusSHA {
		t.Fatalf("reconstructed equivalent H2 corpus drift: sha256=%s pinned=%s", checksum, issue2564H2CorpusSHA)
	}
	t.Logf("H2_EQUIVALENT_CORPUS_SHA256 %s", checksum)
	t.Log("equivalence_authority=https://github.com/division-sh/swarm/issues/2564#issuecomment-6001398406; archive_identity=NOT_UNCHANGED; bounded HTTP transport; independent submission rates are not durable admission rates; six keyed children in one root run; system-node handler writes; explicit hub.start counter initialization; two alternating nonterminal 1s stage timers; close is explicit")
	return root
}

func issue2564H2Harness(t *testing.T, backend, root string) (func(bool) (*channelOnboardingCrashServeProcess, issue2564H2Fixture), *os.File, *os.File) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	var config, location string
	if backend == "postgres" {
		location = storetest.PostgresFixtureLocation(t)
		config = writeChannelOnboardingPostgresRuntimeConfig(t, location)
	} else {
		location = filepath.Join(t.TempDir(), "h2.sqlite")
		config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, location, channelOnboardingHostWorkspaceFields())
	}
	var observations *storetest.Issue2564WorkloadObservation
	setServeRuntimeRecovery(t, config, false, true)
	armR, armW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cutR, cutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{armR, armW, cutR, cutW} {
		t.Cleanup(func() { _ = file.Close() })
	}
	temporary := t.TempDir()
	// As in the existing abrupt process helpers, unlock only the killed child's
	// test-owned read-only source projection for testing.TempDir cleanup.
	t.Cleanup(func() {
		if err := filepath.WalkDir(temporary, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		}); err != nil {
			t.Error(err)
		}
	})
	return func(cut bool) (*channelOnboardingCrashServeProcess, issue2564H2Fixture) {
		raw, err := json.Marshal(issue2564H2Child{Source: root, Config: config, Backend: backend, Cut: cut})
		if err != nil {
			t.Fatal(err)
		}
		process := startServedCrashProcess(t, "TestIssue2564H2ServeProcessHelper", []string{issue2564H2ChildEnv + "=" + string(raw), "TMPDIR=" + temporary}, armR, cutW)
		endpoint := process.endpoint(t) + "/v1/rpc"
		if observations == nil {
			observations = storetest.OpenIssue2564WorkloadObservation(t, backend, location)
		}
		return process, issue2564H2Fixture{Endpoint: endpoint, selected: observations.Reader, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, root)}
	}, armW, cutR
}

type issue2564H2Fixture struct {
	Endpoint, Backend, BundleHash string
	selected                      storetest.Issue2564WorkloadReader
}

func (f issue2564H2Fixture) debug(t *testing.T, runID string) string {
	t.Helper()
	report, err := f.selected.LoadRunDebugReport(context.Background(), runID, operatorread.RunDebugQueryOptions{})
	if err != nil {
		return fmt.Sprintf("H2 run debug report: %v", err)
	}
	return fmt.Sprintf("%+v", report)
}

func (f issue2564H2Fixture) waitDeliveries(t *testing.T, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	stable := 0
	for time.Now().Before(deadline) {
		summary, err := f.selected.SummarizeRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("count active served run deliveries: %v", err)
		}
		if summary.Pending+summary.InProgress == 0 {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("served run %s deliveries did not remain quiescent\n%s", runID, f.debug(t, runID))
}

type issue2564H2Hub struct {
	ID, Entity, Instance, Stage string
	Count, C1, C2, Revision     int64
	History                     []pipeline.WorkflowTransitionRecord
}

type issue2564H2Timer struct {
	ID, Entity, Instance, Status string
	Ref                          timeridentity.WorkflowTimerActivationRef
	Created, Due, Fired          time.Time
}

type issue2564H2Event struct {
	ID, Task, Instance, Outcome, Reason string
	Occurrence                          timeridentity.WorkflowTimerOccurrenceRef
}

type issue2564H2Snapshot struct {
	Hubs   map[string]issue2564H2Hub
	Timers map[string]issue2564H2Timer
	Events map[string]issue2564H2Event
}

func issue2564H2Counters(raw []byte) (string, int64, int64, int64, error) {
	var fields struct {
		HubID string       `json:"hub_id"`
		Count *json.Number `json:"count"`
		C1    *json.Number `json:"c1"`
		C2    *json.Number `json:"c2"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", 0, 0, 0, err
	}
	if fields.HubID == "" || fields.Count == nil || fields.C1 == nil || fields.C2 == nil {
		return "", 0, 0, 0, fmt.Errorf("H2 hub.start did not initialize exact counters: %s", raw)
	}
	count, e1 := fields.Count.Int64()
	c1, e2 := fields.C1.Int64()
	c2, e3 := fields.C2.Int64()
	if e1 != nil || e2 != nil || e3 != nil || count < 0 || c1 < 0 || c2 < 0 || c1+c2 != count {
		return "", 0, 0, 0, fmt.Errorf("H2 inconsistent integer counters: %s", raw)
	}
	return fields.HubID, count, c1, c2, nil
}

func issue2564H2Read(ctx context.Context, rt issue2564H2Fixture, run string) (issue2564H2Snapshot, error) {
	snapshot := issue2564H2Snapshot{Hubs: map[string]issue2564H2Hub{}, Timers: map[string]issue2564H2Timer{}, Events: map[string]issue2564H2Event{}}
	evidence, err := storetest.ObserveH2WorkloadSnapshot(ctx, rt.selected, run)
	if err != nil {
		return issue2564H2Snapshot{}, err
	}
	for _, row := range evidence.Hubs {
		hub := issue2564H2Hub{Entity: row.Entity, Instance: row.Instance, Stage: row.Stage, Revision: row.Revision}
		hub.ID, hub.Count, hub.C1, hub.C2, err = issue2564H2Counters(row.Fields)
		if err != nil {
			return issue2564H2Snapshot{}, err
		}
		if hub.Stage != row.HeaderState || hub.Revision != row.HeaderRevision || snapshot.Hubs[hub.ID].ID != "" {
			return issue2564H2Snapshot{}, fmt.Errorf("H2 header/field identity disagreement for %+v", hub)
		}
		var persisted struct {
			History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
		}
		if err := json.Unmarshal(row.Config, &persisted); err != nil {
			return issue2564H2Snapshot{}, err
		}
		hub.History = persisted.History
		snapshot.Hubs[hub.ID] = hub
	}
	for _, row := range evidence.Timers {
		timer := issue2564H2Timer{ID: row.ID, Entity: row.Entity, Instance: row.Instance, Status: row.Status, Created: row.Created, Due: row.Due, Fired: row.Fired}
		var valid bool
		timer.Ref, valid = timeridentity.ParseWorkflowTimerActivationTaskID(row.Name)
		if !valid || timer.Ref.ActivationID != timer.ID {
			return issue2564H2Snapshot{}, fmt.Errorf("H2 invalid exact timer identity: %+v", timer)
		}
		snapshot.Timers[timer.ID] = timer
	}
	for _, row := range evidence.Events {
		event := issue2564H2Event{ID: row.ID, Task: row.Task, Instance: row.Instance, Outcome: row.Outcome, Reason: row.Reason}
		var valid bool
		event.Occurrence, valid = timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.Task)
		if !valid || timeridentity.WorkflowTimerOccurrenceEventID(event.Occurrence) != event.ID {
			return issue2564H2Snapshot{}, fmt.Errorf("H2 publication lacks exact occurrence identity: %+v", event)
		}
		snapshot.Events[event.ID] = event
	}
	return snapshot, nil
}

func issue2564H2DeclarationKeys(t *testing.T, root string) map[string]string {
	t.Helper()
	bundle := loadWorkflowValidationBundleAt(t, root)
	graph, found := bundle.WorkflowStageTopology("hub")
	if !found {
		t.Fatal("H2 compiled hub topology missing")
	}
	keys := map[string]string{}
	for _, timer := range bundle.WorkflowTimers() {
		if timer.OwningFlowID() != "hub" || !timer.StageOwned {
			continue
		}
		owner, found := bundle.WorkflowStageTimerByID("hub", timer.ID)
		if !found || owner.SemanticKey() == "" || owner.Delay != "1s" || owner.Recurring || keys[owner.Stage] != "" {
			t.Fatalf("H2 admitted compiled timer owner invalid: %+v", owner)
		}
		next := map[string]string{"s1": "s2", "s2": "s1"}[owner.Stage]
		if next == "" || owner.AdvancesTo != next {
			t.Fatalf("H2 compiled timer must alternate nonterminal stages: %+v", owner)
		}
		if _, err := graph.AdmitTransition(runtimecontracts.WorkflowTransitionSite{TimerID: owner.ID}, owner.Stage, next); err != nil {
			t.Fatal(err)
		}
		keys[owner.Stage] = owner.SemanticKey()
		t.Logf("H2 admitted_timer_owner stage=%s id=%s declaration_key=%s", owner.Stage, owner.ID, owner.SemanticKey())
	}
	if len(keys) != 2 {
		t.Fatalf("H2 compiled alternating timer owners=%v, want exactly s1/s2", keys)
	}
	return keys
}

func issue2564H2Accounting(run string, snapshot issue2564H2Snapshot, closed bool, keys map[string]string) error {
	if len(snapshot.Hubs) != 6 {
		return fmt.Errorf("H2 hub inventory=%d, want six", len(snapshot.Hubs))
	}
	seen := map[string]bool{}
	for _, hub := range snapshot.Hubs {
		stage, initial, transitions, active, timerRows := "s1", 0, 0, 0, 0
		for _, timer := range snapshot.Timers {
			if timer.Entity != hub.Entity {
				continue
			}
			timerRows++
			if timer.Status != "active" && timer.Status != "fired" && !(closed && timer.Status == "cancelled") {
				return fmt.Errorf("H2 unexpected timer status: %+v", timer)
			}
			if timer.Instance != hub.Instance || timer.Due.Sub(timer.Created) != time.Second {
				return fmt.Errorf("H2 changed route/one-second interval: %+v", timer)
			}
			if timer.Ref.Cause == timeridentity.WorkflowTimerActivationCauseInitial {
				initial++
				if timer.Ref.DeclarationKey != keys["s1"] {
					return fmt.Errorf("H2 wrong initial entry: %+v", timer)
				}
			}
			if timer.Status == "active" {
				active++
				if timer.Ref.DeclarationKey != keys[hub.Stage] {
					return fmt.Errorf("H2 live successor disagrees with current entry: %+v hub=%+v", timer, hub)
				}
			}
		}
		for _, record := range hub.History {
			if err := record.Evidence.Validate(); err != nil {
				return err
			}
			compiled, ok := record.Evidence.Compiled()
			if !ok || compiled.Edge().Source != "timer" {
				if !(record.From == "" && record.To == "s1") && !(closed && record.To == "closed") {
					return fmt.Errorf("H2 unexpected non-timer transition: hub=%s record=%+v", hub.ID, record)
				}
				continue
			}
			transitions++
			event, found := snapshot.Events[record.TriggerEventID]
			timer := snapshot.Timers[event.Occurrence.Activation.ActivationID]
			next := "s2"
			if stage == "s2" {
				next = "s1"
			}
			if !found || seen[event.ID] || event.Instance != hub.Instance || event.Outcome != "success" || timer.Entity != hub.Entity || timer.Status != "fired" || timer.Ref != event.Occurrence.Activation || !timer.Due.Equal(event.Occurrence.DueAt) || record.From != stage || record.To != next || record.TransitionID != record.Evidence.ID() || timer.Ref.DeclarationKey != keys[stage] || compiled.FlowID() != "hub" {
				return fmt.Errorf("H2 accepted-occurrence/transition accounting failed: hub=%s record=%+v event=%+v timer=%+v", hub.ID, record, event, timer)
			}
			seen[event.ID] = true
			successors := 0
			for _, successor := range snapshot.Timers {
				if successor.Entity != hub.Entity || successor.Ref.DeclarationKey != keys[next] {
					continue
				}
				want := timeridentity.WorkflowTimerActivationID(run, hub.Entity, hub.Instance, successor.Ref.DeclarationKey, successor.Ref.DeclarationRevision, string(timeridentity.WorkflowTimerActivationCauseTransition), successor.Ref.Generation.KeySuffix(), event.ID, "platform.stage_timer", record.TransitionID, stage, next)
				if successor.ID == want && successor.Ref.Cause == timeridentity.WorkflowTimerActivationCauseTransition && successor.Created.Equal(record.FiredAt) {
					successors++
				}
			}
			if successors != 1 {
				return fmt.Errorf("H2 occurrence %s has %d exact entry successors, want one", event.ID, successors)
			}
			stage = next
		}
		if initial != 1 || transitions == 0 || timerRows != transitions+1 || (!closed && (hub.Stage != stage || active != 1)) || (closed && (hub.Stage != "closed" || active != 0)) {
			return fmt.Errorf("H2 initial/current entry not exact: hub=%s stage=%s expected=%s initial=%d transitions=%d active=%d closed=%v", hub.ID, hub.Stage, stage, initial, transitions, active, closed)
		}
	}
	if len(seen) != len(snapshot.Events) {
		return fmt.Errorf("H2 accepted occurrences=%d, exactly applied transitions=%d (unadvanced/stale timer forbidden)", len(snapshot.Events), len(seen))
	}
	return nil
}

func issue2564H2WaitAccounting(t *testing.T, rt issue2564H2Fixture, run string, closed bool, keys map[string]string) issue2564H2Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var last error
	for ctx.Err() == nil {
		snapshot, err := issue2564H2Read(ctx, rt, run)
		if err == nil {
			err = issue2564H2Accounting(run, snapshot, closed, keys)
			if err == nil {
				return snapshot
			}
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("exact H2 accounting never settled: %v\n%s", last, rt.debug(t, run))
	return issue2564H2Snapshot{}
}

func issue2564H2Publish(ctx context.Context, client *http.Client, endpoint string, params map[string]any) (servedEventPublishRPCResult, error) {
	var result servedEventPublishRPCResult
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": params["idempotency_key"], "method": "event.publish", "params": params})
	if err != nil {
		return result, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	response, err := client.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return result, err
	}
	if response.StatusCode != http.StatusOK || envelope.Error != nil {
		return result, fmt.Errorf("H2 publish status=%d error=%+v", response.StatusCode, envelope.Error)
	}
	err = json.Unmarshal(envelope.Result, &result)
	return result, err
}

func issue2564H2QualificationContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	deadline, bounded := t.Deadline()
	if !bounded {
		t.Fatal("H2 served workload requires the qualification's bounded go test deadline")
	}
	return context.WithDeadline(t.Context(), deadline)
}

func issue2564H2HTTPClient(t *testing.T, rt issue2564H2Fixture) *http.Client {
	t.Helper()
	limit := 64
	if rt.Backend == "postgres" {
		capacity, err := storetest.ObserveH2ServerCapacity(context.Background(), rt.selected)
		if err != nil {
			t.Fatal(err)
		}
		// Leave capacity for independent claim/advisory sessions and runtime work.
		limit = min(limit, capacity/8)
		t.Logf("H2 transport_capacity postgres_max_connections=%d max_connections_per_host=%d", capacity, limit)
	}
	if limit < 6 {
		t.Fatalf("H2 transport capacity=%d cannot overlap all six hubs", limit)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = limit, limit
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}
}

func issue2564H2PGSessionSampler(t *testing.T, ctx context.Context, rt issue2564H2Fixture, run string, count int, acknowledged *sync.Map) (func(string), func()) {
	t.Helper()
	if rt.Backend != "postgres" {
		return func(string) {}, func() {}
	}
	// Retain the native fixture's private read connection before pressure.
	observation, err := storetest.BeginH2SessionObservation(ctx, rt.selected, run, count)
	if err != nil {
		t.Fatal(err)
	}
	return func(label string) {
			var acked []string
			acknowledged.Range(func(event, _ any) bool { acked = append(acked, event.(string)); return true })
			sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			samples, err := observation.SampleForTest(sampleCtx, acked)
			if err != nil {
				t.Logf("H2 native_pg_sessions phase=%s acknowledged_HTTP=%d error=%v", label, len(acked), err)
				return
			}
			total, pipeline, api, ackedClaims := 0, 0, 0, 0
			for _, sample := range samples {
				database, application, state, owner := sample.Database, sample.Application, sample.State, sample.Owner
				sessions, keys, acknowledgedKeys, age := sample.Sessions, sample.Keys, sample.AcknowledgedKeys, sample.Age
				total += sessions
				if owner == "pipeline_only" || owner == "pipeline+api" {
					pipeline += sessions
				}
				if owner == "api_only" || owner == "pipeline+api" {
					api += sessions
				}
				ackedClaims += acknowledgedKeys
				t.Logf("H2 native_pg_sessions phase=%s database=%s application=%q state=%s owner=%s sessions=%d pipeline_event_keys=%d HTTP_acknowledged_pipeline_keys=%d max_state_age_seconds=%.3f", label, database, application, state, owner, sessions, keys, acknowledgedKeys, age)
			}
			t.Logf("H2 native_pg_session_totals phase=%s client_sessions=%d pipeline_owner_sessions=%d api_owner_sessions=%d acknowledged_HTTP=%d still_held_ACKed_event_keys=%d sampler_backend_excluded=true", label, total, pipeline, api, len(acked), ackedClaims)
		}, func() {
			if err := observation.CloseForTest(); err != nil {
				t.Errorf("H2 native_pg_sessions close: %v", err)
			}
		}
}

func issue2564H2Bumps(t *testing.T, rt issue2564H2Fixture, seed servedEventPublishRPCResult, count, rate int) map[string]string {
	t.Helper()
	ctx, cancel := issue2564H2QualificationContext(t)
	defer cancel()
	var acknowledged sync.Map
	samplePG, releaseSampler := issue2564H2PGSessionSampler(t, ctx, rt, seed.RunID, count, &acknowledged)
	defer releaseSampler()
	samplePG("before_bumps")
	monitorDone := make(chan struct{})
	defer func() { cancel(); <-monitorDone }()
	go func() {
		defer close(monitorDone)
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		var nativeTick <-chan time.Time
		if rt.Backend == "postgres" {
			native := time.NewTicker(2 * time.Second)
			defer native.Stop()
			nativeTick = native.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-nativeTick:
				samplePG("during_bumps")
			case <-tick.C:
				rows, err := storetest.ObserveH2ResponseQueue(ctx, rt.selected, seed.RunID)
				var progress []string
				for _, row := range rows {
					progress = append(progress, fmt.Sprintf("%s/%s=%d", row.Name, row.Status.String, row.Count))
				}
				if err != nil {
					progress = append(progress, err.Error())
				}
				t.Logf("H2 live response-queue diagnostic: %s", strings.Join(progress, " "))
			}
		}
	}()
	type reply struct {
		event, hub                           string
		at, connected, written, acknowledged time.Time
		err                                  error
	}
	replies := make(chan reply, count)
	// Requests use the existing qualification deadline, not an additional ACK
	// timeout. Input pacing remains independent of the serialized response queue.
	client := issue2564H2HTTPClient(t, rt)
	var workers sync.WaitGroup
	// Independent real HTTP publishers overlap ordinary served node execution.
	// The pacing clock is not coupled to handler completion or timer scheduling.
	tick := time.NewTicker(time.Second / time.Duration(rate))
	for ordinal := 0; ordinal < count; ordinal++ {
		if ordinal != 0 {
			select {
			case <-tick.C:
			case <-ctx.Done():
				workers.Wait()
				var firstFailure error
				completed := len(replies)
				for len(replies) > 0 {
					reply := <-replies
					if firstFailure == nil && reply.err != nil {
						firstFailure = reply.err
					}
				}
				persistedEvidence, persistedErr := storetest.ObserveH2EventAccounting(context.Background(), rt.selected, seed.RunID)
				if persistedErr != nil {
					t.Logf("H2 persisted-event diagnostic read: %v", persistedErr)
				}
				summary, pendingErr := rt.selected.SummarizeRun(context.Background(), seed.RunID)
				if pendingErr != nil {
					t.Logf("H2 pending-delivery diagnostic read: %v", pendingErr)
				}
				persisted, pending := persistedEvidence.Events, summary.Pending+summary.InProgress
				t.Fatalf("H2 workload pacing deadline expired: issued=%d/%d completed=%d persisted_events=%d pending_deliveries=%d first_error=%v\n%s", ordinal, count, completed, persisted, pending, firstFailure, rt.debug(t, seed.RunID))
			}
		}
		workers.Add(1)
		go func(ordinal int) {
			defer workers.Done()
			hub := fmt.Sprintf("h%02d", ordinal%6+1)
			at := time.Now()
			var connected, written atomic.Int64
			requestContext := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
				GotConn:      func(httptrace.GotConnInfo) { connected.Store(time.Now().UnixNano()) },
				WroteRequest: func(httptrace.WroteRequestInfo) { written.Store(time.Now().UnixNano()) },
			})
			result, err := issue2564H2Publish(requestContext, client, rt.Endpoint, map[string]any{"event_name": "hub.bump", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": hub, "n": ordinal + 1}, "idempotency_key": fmt.Sprintf("h2-bump-%04d", ordinal)})
			if err == nil && (result.RunID != seed.RunID || result.EventID == "") {
				err = fmt.Errorf("H2 bump changed run/empty event: %+v", result)
			}
			if err == nil {
				acknowledged.Store(result.EventID, struct{}{})
			}
			replies <- reply{event: result.EventID, hub: hub, at: at, connected: time.Unix(0, connected.Load()), written: time.Unix(0, written.Load()), acknowledged: time.Now(), err: err}
		}(ordinal)
	}
	tick.Stop()
	workers.Wait()
	close(replies)
	accepted := map[string]string{}
	failures := map[string]int{}
	var first, last, firstWire, lastWire, firstACK, lastACK time.Time
	var totalQueue, maxQueue time.Duration
	for reply := range replies {
		if first.IsZero() || reply.at.Before(first) {
			first = reply.at
		}
		if reply.at.After(last) {
			last = reply.at
		}
		if reply.err != nil {
			failures[reply.err.Error()]++
			continue
		}
		if reply.connected.UnixNano() == 0 || reply.written.UnixNano() == 0 {
			t.Fatal("H2 acknowledged HTTP request lacks transport evidence")
		}
		queue := reply.connected.Sub(reply.at)
		totalQueue += queue
		maxQueue = max(maxQueue, queue)
		if firstWire.IsZero() || reply.written.Before(firstWire) {
			firstWire = reply.written
		}
		lastWire = issue2564H2MaxTime(lastWire, reply.written)
		if firstACK.IsZero() || reply.acknowledged.Before(firstACK) {
			firstACK = reply.acknowledged
		}
		lastACK = issue2564H2MaxTime(lastACK, reply.acknowledged)
		if accepted[reply.event] != "" {
			t.Fatalf("H2 duplicated accepted bump %s", reply.event)
		}
		accepted[reply.event] = reply.hub
	}
	observed := float64(count-1) / last.Sub(first).Seconds()
	if len(failures) != 0 {
		t.Fatalf("H2 issued=%d accepted=%d input_rate=%.3f/s span=%s RPC_errors=%v\n%s", count, len(accepted), observed, last.Sub(first), failures, rt.debug(t, seed.RunID))
	}
	if len(accepted) != count || observed < float64(rate)*0.85 || observed > float64(rate)*1.15 {
		t.Fatalf("H2 workload changed: accepted=%d/%d observed_rate=%.2f target=%d", len(accepted), count, observed, rate)
	}
	t.Logf("reconstructed_equivalent_H2 accepted_bumps=%d independent_requests=%d observed_input_rate=%.3f/s target=%d/s span=%s", count, count, observed, rate, last.Sub(first))
	t.Logf("H2 transport_observation wire_request_rate=%.3f/s ACK_rate=%.3f/s connection_wait_mean=%s connection_wait_max=%s submission_rate_is_NOT_admission_rate=true", float64(count-1)/lastWire.Sub(firstWire).Seconds(), float64(count-1)/lastACK.Sub(firstACK).Seconds(), totalQueue/time.Duration(count), maxQueue)
	return accepted
}

func issue2564H2MaxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func issue2564H2AdmissionEvidence(t *testing.T, output string, accepted map[string]string) {
	t.Helper()
	seen := map[string]bool{}
	var first, last time.Time
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "H2_DURABLE_ADMISSION ") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 {
			t.Fatalf("H2 malformed post-commit observation: %q", line)
		}
		event := strings.TrimPrefix(parts[1], "event=")
		at, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(parts[2], "observed_at="))
		if err != nil || accepted[event] == "" || seen[event] {
			t.Fatalf("H2 post-commit observation not one exact accepted bump: %q error=%v", line, err)
		}
		seen[event] = true
		if first.IsZero() || at.Before(first) {
			first = at
		}
		last = issue2564H2MaxTime(last, at)
	}
	if len(seen) != len(accepted) {
		t.Fatalf("H2 exact durable admission observations=%d accepted=%d", len(seen), len(accepted))
	}
	t.Logf("H2 durable_publication_observation exact_events=%d observed_postcommit_rate=%.3f/s span=%s (not a claim of submission-rate durable admissions)", len(seen), float64(len(seen)-1)/last.Sub(first).Seconds(), last.Sub(first))
}

func issue2564H2CounterEvidence(t *testing.T, rt issue2564H2Fixture, run string, accepted map[string]string, snapshot issue2564H2Snapshot) {
	t.Helper()
	rows, err := storetest.ObserveH2CounterMutations(context.Background(), rt.selected, run)
	if err != nil {
		t.Fatal(err)
	}
	chains, effects := map[string]map[int64]int64{}, map[string]map[string]int{}
	for _, row := range rows {
		entity, path, cause, old, next := row.Entity, row.Path, row.Cause, row.Before, row.After
		var before, after int64
		if !next.Valid || json.Unmarshal([]byte(next.String), &after) != nil {
			t.Fatalf("H2 counter became absent/noninteger: %s %s -> %s", path, old.String, next.String)
		}
		if !old.Valid || old.String == "null" {
			if after != 0 {
				t.Fatalf("H2 uninitialized counter skipped zero: %s/%s -> %d", entity, path, after)
			}
			continue
		}
		if err := json.Unmarshal([]byte(old.String), &before); err != nil || after < before {
			t.Fatalf("H2 counter reversion: entity=%s path=%s before=%s after=%s cause=%s", entity, path, old.String, next.String, cause)
		}
		if before == after {
			continue
		}
		if accepted[cause] == "" || after != before+1 {
			t.Fatalf("H2 counter change lacks one real bump: entity=%s path=%s %d->%d cause=%s", entity, path, before, after, cause)
		}
		if snapshot.Hubs[accepted[cause]].Entity != entity {
			t.Fatalf("H2 mutation escaped exact keyed receiver: event=%s hub=%s entity=%s", cause, accepted[cause], entity)
		}
		key := entity + ":" + path
		if chains[key] == nil {
			chains[key] = map[int64]int64{}
		}
		if _, duplicate := chains[key][before]; duplicate {
			t.Fatalf("H2 repeated stale counter evaluation: %s before=%d", key, before)
		}
		chains[key][before] = after
		if effects[cause] == nil {
			effects[cause] = map[string]int{}
		}
		effects[cause][path]++
	}
	for event, hubID := range accepted {
		if effects[event]["count"] != 1 || effects[event]["c1"]+effects[event]["c2"] != 1 {
			t.Fatalf("H2 bump %s hub=%s does not have exact coupled count/stage evidence: %v", event, hubID, effects[event])
		}
	}
	for _, hub := range snapshot.Hubs {
		if hub.Count != int64(len(accepted)/6) || hub.C1 == 0 || hub.C2 == 0 {
			t.Fatalf("H2 exact per-hub bump/stage counts: %+v expected=%d", hub, len(accepted)/6)
		}
		for path, final := range map[string]int64{"count": hub.Count, "c1": hub.C1, "c2": hub.C2} {
			chain := chains[hub.Entity+":"+path]
			if int64(len(chain)) != final {
				t.Fatalf("H2 counter mutation evidence lost: %s/%s chain=%d final=%d", hub.ID, path, len(chain), final)
			}
			for before := int64(0); before < final; before++ {
				if chain[before] != before+1 {
					t.Fatalf("H2 nonmonotonic/disconnected acknowledged history: %s/%s at %d", hub.ID, path, before)
				}
			}
		}
	}
}

func issue2564H2Deliveries(t *testing.T, rt issue2564H2Fixture, run string, accepted map[string]string, userEvents int) {
	t.Helper()
	rows, err := storetest.ObserveH2NodeDeliveries(context.Background(), rt.selected, run)
	if err != nil {
		t.Fatal(err)
	}
	total, bumped := 0, map[string]bool{}
	for _, row := range rows {
		event, status, name, retries, payload := row.Event, row.Status, row.Name, row.Retries, row.Payload
		total++
		if status != "delivered" || retries != 0 {
			t.Fatalf("H2 stale-entry/contention delivery: event=%s type=%s status=%s retry_count=%d", event, name, status, retries)
		}
		if hub := accepted[event]; hub != "" {
			var body struct {
				HubID string `json:"hub_id"`
			}
			if err := json.Unmarshal(payload, &body); err != nil || body.HubID != hub || bumped[event] {
				t.Fatalf("H2 exact keyed delivery mismatch: event=%s hub=%s payload=%s", event, hub, payload)
			}
			bumped[event] = true
		}
	}
	if total != userEvents || len(bumped) != len(accepted) {
		t.Fatalf("H2 real node delivery counts=%d/%d bumps=%d/%d", total, userEvents, len(bumped), len(accepted))
	}
	failures, err := storetest.ObserveH2DeliveryAccounting(context.Background(), rt.selected, run)
	if err != nil {
		t.Fatal(err)
	}
	dead, retried := failures.DeadLetters, failures.Retried
	if dead != 0 || retried != 0 {
		t.Fatalf("H2 handler failure evidence: dead_letters=%d retry/dead_letter_attempts=%d", dead, retried)
	}
}

func TestIssue2564ReconstructedEquivalentH2BothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, workload := range []struct {
			name        string
			bumps, rate int
		}{{"dense", 900, 40}, {"paced", 600, 13}} {
			t.Run(backend+"/"+workload.name, func(t *testing.T) {
				root := issue2564H2Source(t)
				keys := issue2564H2DeclarationKeys(t, root)
				start, arm, cut := issue2564H2Harness(t, backend, root)
				first, rt := start(true)
				t.Cleanup(func() {
					if t.Failed() {
						_, _ = arm.Write([]byte{2})
						until := time.Now().Add(2 * time.Second)
						for time.Now().Before(until) && !strings.Contains(first.output.String(), "H2_STACK_DUMP_END") {
							time.Sleep(10 * time.Millisecond)
						}
						pendingEvidence, pendingErr := storetest.ObserveH2PendingAccounting(context.Background(), rt.selected, "")
						if pendingErr != nil {
							t.Logf("H2 failure pending-delivery read: %v", pendingErr)
						}
						pending := pendingEvidence.Pending
						t.Logf("H2 failure pending_deliveries=%d", pending)
						t.Logf("H2 first process raw output:\n%s", first.output.String())
					}
				})
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "h2-start-1"})
				for hub := 2; hub <= 6; hub++ {
					requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": fmt.Sprintf("h%02d", hub)}, "idempotency_key": fmt.Sprintf("h2-start-%d", hub)})
				}
				rt.waitDeliveries(t, seed.RunID)
				initialized, err := issue2564H2Read(context.Background(), rt, seed.RunID)
				if err != nil || len(initialized.Hubs) != 6 {
					t.Fatalf("H2 real six-hub construction: hubs=%d error=%v", len(initialized.Hubs), err)
				}
				for _, hub := range initialized.Hubs {
					if hub.Count != 0 || hub.C1 != 0 || hub.C2 != 0 {
						t.Fatalf("H2 hub.start initialization failed: %+v", hub)
					}
					initial := 0
					for _, timer := range initialized.Timers {
						if timer.Entity == hub.Entity && timer.Ref.Cause == timeridentity.WorkflowTimerActivationCauseInitial {
							initial++
							if timer.Ref.DeclarationKey != keys["s1"] || timer.Due.Sub(timer.Created) != time.Second {
								t.Fatalf("H2 initial compiled timer declaration/interval mismatch: %+v", timer)
							}
						}
					}
					if initial != 1 {
						t.Fatalf("H2 hub %s has %d initial-entry timers, want exactly one", hub.ID, initial)
					}
				}
				constructionEvidence, err := storetest.ObserveH2ConstructionAccounting(context.Background(), rt.selected, seed.RunID)
				constructions := constructionEvidence.Constructions
				if err != nil || constructions != 7 {
					t.Fatalf("H2 requires one root plus six real keyed-child constructions: %d %v", constructions, err)
				}
				accepted := issue2564H2Bumps(t, rt, seed, workload.bumps, workload.rate)
				issue2564H2AdmissionEvidence(t, first.output.String(), accepted)
				rt.waitDeliveries(t, seed.RunID)
				before := issue2564H2WaitAccounting(t, rt, seed.RunID, false, keys)
				issue2564H2Deliveries(t, rt, seed.RunID, accepted, workload.bumps+6)
				issue2564H2CounterEvidence(t, rt, seed.RunID, accepted, before)
				if _, err := arm.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				reported := make(chan lifecycleprobe.Signal, 1)
				failed := make(chan error, 1)
				go func() {
					var signal lifecycleprobe.Signal
					if err := json.NewDecoder(cut).Decode(&signal); err != nil {
						failed <- err
						return
					}
					reported <- signal
				}()
				var signal lifecycleprobe.Signal
				select {
				case signal = <-reported:
				case err := <-failed:
					t.Fatal(err)
				case <-time.After(15 * time.Second):
					t.Fatalf("H2 exact occurrence publication cut not reached\n%s", first.output.String())
				}
				interrupted, err := issue2564H2Read(context.Background(), rt, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				event := interrupted.Events[signal.EventID]
				fired := interrupted.Timers[event.Occurrence.Activation.ActivationID]
				if event.ID == "" || event.Outcome != "" || fired.Status != "fired" || fired.Fired.IsZero() {
					t.Fatalf("H2 cut is not published-but-unadvanced: signal=%+v event=%+v timer=%+v", signal, event, fired)
				}
				for _, timer := range interrupted.Timers {
					if timer.Entity == fired.Entity && timer.Status == "active" {
						t.Fatalf("H2 cut already armed a successor before transition: %+v", timer)
					}
				}
				for _, hub := range interrupted.Hubs {
					for _, record := range hub.History {
						if record.TriggerEventID == event.ID {
							t.Fatal("H2 cut already advanced before abrupt process death")
						}
					}
				}
				if err := first.kill(); err != nil || first.waitError() == nil {
					t.Fatalf("H2 did not die abruptly: kill=%v wait=%v", err, first.waitError())
				}
				second, rt := start(false)
				// No event.publish (not even a duplicate) occurs between SIGKILL and
				// this exact accepted event/transition/successor recovery assertion.
				recovered := issue2564H2WaitAccounting(t, rt, seed.RunID, false, keys)
				if recovered.Events[event.ID].Task != event.Task || !reflect.DeepEqual(recovered.Timers[fired.ID], fired) {
					t.Fatal("H2 restart reset/reminted the exact committed occurrence")
				}
				for id, old := range interrupted.Hubs {
					next := recovered.Hubs[id]
					if old.Entity != next.Entity || old.Instance != next.Instance || old.Count != next.Count || old.C1 != next.C1 || old.C2 != next.C2 || len(next.History) < len(old.History) || !reflect.DeepEqual(old.History, next.History[:len(old.History)]) {
						t.Fatalf("H2 non-dev restart reverted/reminted a hub: before=%+v after=%+v", old, next)
					}
				}
				issue2564H2CounterEvidence(t, rt, seed.RunID, accepted, recovered)
				t.Logf("reconstructed_equivalent_H2 nondev_SIGKILL_reopen event=%s task=%s activation=%s accepted_before=%d accepted_after=%d no_extra_user_event=true", event.ID, event.Task, fired.ID, len(interrupted.Events), len(recovered.Events))
				// Live occurrence/transition/successor accounting is already proven.
				// Explicit cleanup may legitimately supersede a concurrently due timer;
				// it must preserve the verified prefix, not require a six-hub quiet phase.
				var closers sync.WaitGroup
				closeErrors := make(chan error, 6)
				closeContext, cancelClose := issue2564H2QualificationContext(t)
				defer cancelClose()
				for hub := 1; hub <= 6; hub++ {
					closers.Add(1)
					go func(hub int) {
						defer closers.Done()
						_, err := issue2564H2Publish(closeContext, &http.Client{}, rt.Endpoint, map[string]any{"event_name": "hub.close", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": fmt.Sprintf("h%02d", hub)}, "idempotency_key": fmt.Sprintf("h2-close-%d", hub)})
						closeErrors <- err
					}(hub)
				}
				closers.Wait()
				close(closeErrors)
				for err := range closeErrors {
					if err != nil {
						t.Fatal(err)
					}
				}
				rt.waitDeliveries(t, seed.RunID)
				closed, err := issue2564H2Read(context.Background(), rt, seed.RunID)
				if err != nil {
					t.Fatal(err)
				}
				for id, old := range recovered.Hubs {
					next := closed.Hubs[id]
					if next.Stage != "closed" || old.Entity != next.Entity || old.Instance != next.Instance || old.Count != next.Count || old.C1 != next.C1 || old.C2 != next.C2 || len(next.History) < len(old.History) || !reflect.DeepEqual(old.History, next.History[:len(old.History)]) {
						t.Fatalf("H2 explicit cleanup changed verified hub prefix: before=%+v after=%+v", old, next)
					}
				}
				for id, old := range recovered.Events {
					if !reflect.DeepEqual(old, closed.Events[id]) {
						t.Fatalf("H2 explicit cleanup changed verified occurrence receipt: before=%+v after=%+v", old, closed.Events[id])
					}
				}
				for _, timer := range closed.Timers {
					if timer.Status == "active" {
						t.Fatalf("H2 explicit cleanup retained an active timer: %+v", timer)
					}
				}
				issue2564H2CounterEvidence(t, rt, seed.RunID, accepted, closed)
				issue2564H2Deliveries(t, rt, seed.RunID, accepted, workload.bumps+12)
				t.Logf("reconstructed_equivalent_H2 final hubs=6 bumps=%d live_verified_timer_occurrences=%d exact_live_transitions=%d cleanup_tail_occurrences=%d closed_explicitly=6 no_reverts=true stale_entry_deadletters=0 retry_count=0", workload.bumps, len(recovered.Events), len(recovered.Events), len(closed.Events)-len(recovered.Events))
				if err := second.stop(); err != nil {
					t.Fatalf("H2 explicit-close process stop: %v\n%s", err, second.output.String())
				}
			})
		}
	}

}

var _ lifecycleprobe.Observer = (*issue2564H2Cut)(nil)

func TestIssue2564H2WorkloadObservationPortsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			start, _, _ := issue2564H2Harness(t, backend, issue2564H2Source(t))
			first, rt := start(false)
			// This exercises observation ports only, not an H2 pressure qualification.
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "hub.start", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "h2-start-1",
			})
			rt.waitDeliveries(t, seed.RunID)
			issue2564WorkloadObservationChecks(t, rt, seed.RunID)
			snapshot, err := issue2564H2Read(t.Context(), rt, seed.RunID)
			if err != nil || len(snapshot.Hubs) != 1 || len(snapshot.Timers) == 0 {
				t.Fatalf("native snapshot observation: %+v error=%v", snapshot, err)
			}
			if backend == "postgres" {
				capacity, err := storetest.ObserveH2ServerCapacity(t.Context(), rt.selected)
				if err != nil || capacity < 6 {
					t.Fatalf("native capacity observation: %d %v", capacity, err)
				}
				var acknowledged sync.Map
				acknowledged.Store(seed.EventID, struct{}{})
				sample, release := issue2564H2PGSessionSampler(t, t.Context(), rt, seed.RunID, 1, &acknowledged)
				sample("observation_port_smoke")
				release()
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			evidence, err := storetest.ObserveH2WorkloadSnapshot(cancelled, rt.selected, seed.RunID)
			if err == nil || !reflect.DeepEqual(evidence, storetest.H2WorkloadSnapshotEvidence{}) {
				t.Fatalf("cancelled snapshot exposed partial evidence: %+v %v", evidence, err)
			}
			if err := first.stop(); err != nil {
				t.Fatal(err)
			}
			second, reopened := start(false)
			after, err := issue2564H2Read(t.Context(), reopened, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			old, next := snapshot.Hubs["h01"], after.Hubs["h01"]
			if old.Entity != next.Entity || old.Instance != next.Instance || old.Count != next.Count || old.C1 != next.C1 || old.C2 != next.C2 {
				t.Fatalf("native location readback changed hub: before=%+v after=%+v", old, next)
			}
			issue2564WorkloadObservationChecks(t, reopened, seed.RunID)
			if err := second.stop(); err != nil {
				t.Fatal(err)
			}
			t.Logf("ISSUE2564_OBSERVATION_PORT_SMOKE backend=%s native_location_reopen=true pressure_qualification=false", backend)
		})
	}
}

func issue2564WorkloadObservationChecks(t *testing.T, rt issue2564H2Fixture, runID string) {
	t.Helper()
	ctx := t.Context()
	checks := []struct {
		name string
		read func() error
	}{
		{"H1FlowAccounting", func() error { _, err := storetest.ObserveH1FlowAccounting(ctx, rt.selected, runID); return err }},
		{"H1TurnAccounting", func() error { _, err := storetest.ObserveH1TurnAccounting(ctx, rt.selected, runID); return err }},
		{"H1BumpAccounting", func() error { _, err := storetest.ObserveH1BumpAccounting(ctx, rt.selected, runID); return err }},
		{"H1DeliveryAccounting", func() error { _, err := storetest.ObserveH1DeliveryAccounting(ctx, rt.selected, runID); return err }},
		{"H2DeliveryAccounting", func() error { _, err := storetest.ObserveH2DeliveryAccounting(ctx, rt.selected, runID); return err }},
		{"H2ConstructionAccounting", func() error { _, err := storetest.ObserveH2ConstructionAccounting(ctx, rt.selected, runID); return err }},
		{"H2EventAccounting", func() error { _, err := storetest.ObserveH2EventAccounting(ctx, rt.selected, runID); return err }},
		{"H2PendingAccounting", func() error { _, err := storetest.ObserveH2PendingAccounting(ctx, rt.selected, runID); return err }},
		{"H1RunOverlap", func() error { _, err := storetest.ObserveH1RunOverlap(ctx, rt.selected, runID); return err }},
		{"H1HubFields", func() error { _, err := storetest.ObserveH1HubFields(ctx, rt.selected, runID); return err }},
		{"H1AttributedMutations", func() error {
			_, err := storetest.ObserveH1AttributedMutations(ctx, rt.selected, runID, rt.BundleHash)
			return err
		}},
		{"H1SameEntityOverlap", func() error { _, err := storetest.ObserveH1SameEntityOverlap(ctx, rt.selected, runID); return err }},
		{"H1FailureMutations", func() error { _, err := storetest.ObserveH1FailureMutations(ctx, rt.selected, runID); return err }},
		{"H1NodeFailures", func() error { _, err := storetest.ObserveH1NodeFailures(ctx, rt.selected, runID); return err }},
		{"H1AttemptFailures", func() error { _, err := storetest.ObserveH1AttemptFailures(ctx, rt.selected, runID); return err }},
		{"H1DeadLetters", func() error { _, err := storetest.ObserveH1DeadLetters(ctx, rt.selected, runID); return err }},
		{"H1BumpHistory", func() error { _, err := storetest.ObserveH1BumpHistory(ctx, rt.selected, runID); return err }},
		{"H2Hubs", func() error { _, err := storetest.ObserveH2Hubs(ctx, rt.selected, runID); return err }},
		{"H2Occurrences", func() error { _, err := storetest.ObserveH2Occurrences(ctx, rt.selected, runID); return err }},
		{"H2ResponseQueue", func() error { _, err := storetest.ObserveH2ResponseQueue(ctx, rt.selected, runID); return err }},
		{"H2CounterMutations", func() error { _, err := storetest.ObserveH2CounterMutations(ctx, rt.selected, runID); return err }},
		{"H2NodeDeliveries", func() error { _, err := storetest.ObserveH2NodeDeliveries(ctx, rt.selected, runID); return err }},
	}
	for _, check := range checks {
		if err := check.read(); err != nil {
			t.Fatalf("closed %s observation: %v", check.name, err)
		}
	}
}

func TestIssue2564H2WorkloadObservationsRejectOtherOwner(t *testing.T) {
	selected := struct{}{}
	if facts, err := storetest.ObserveH1DeliveryAccounting(t.Context(), selected, "run"); err == nil || facts != (storetest.H1DeliveryAccountingEvidence{}) {
		t.Fatalf("foreign delivery owner exposed evidence: %+v %v", facts, err)
	}
	if rows, err := storetest.ObserveH1FailureMutations(t.Context(), selected, "run"); err == nil || rows != nil {
		t.Fatalf("foreign mutation owner exposed evidence: %+v %v", rows, err)
	}
	if facts, err := storetest.ObserveH2WorkloadSnapshot(t.Context(), selected, "run"); err == nil || !reflect.DeepEqual(facts, storetest.H2WorkloadSnapshotEvidence{}) {
		t.Fatalf("foreign snapshot owner exposed evidence: %+v %v", facts, err)
	}
	if facts, err := storetest.BeginH2SessionObservation(t.Context(), selected, "run", 1); err == nil || facts != nil {
		t.Fatalf("foreign physical observation owner exposed evidence: %+v %v", facts, err)
	}
}
