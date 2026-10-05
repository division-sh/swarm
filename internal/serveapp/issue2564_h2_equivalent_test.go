package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

// Reconstructed equivalent H2, NOT the unchanged lead-local archive. Sources
// below reconstruct #2564's published root/keyed-child bundle under the explicit
// substitute permission in issuecomment-6001398406. Explicit timer IDs, current
// unquoted CEL slots, and this bounded-transport RPC/process driver are declared
// differences. Six hubs, 1s intervals, 900/40s^-1 and 600/13s^-1 submissions,
// and counter initialization are preserved; actual admission rates are separate.
// Deterministic old-head H2a/H2b/H2c mechanism witnesses remain separate proofs.
const issue2564H2ChildEnv = "SWARM_TEST_ISSUE2564_H2_CHILD"

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
	files := map[string]string{
		"schema.yaml": `name: lu-h2-equivalent
pins: {inputs: [hub.start, hub.bump, hub.close]}
connect:
  - {event: hub.start, from: ., to: hub, resolution: select-or-create}
  - {event: hub.bump, from: ., to: hub, resolution: select}
  - {event: hub.close, from: ., to: hub, resolution: select}
`,
		"events.yaml": "hub.start: {key: hub_id, hub_id: text}\nhub.bump: {key: hub_id, hub_id: text, n: integer}\nhub.close: {key: hub_id, hub_id: text}\n",
		"hub/schema.yaml": `name: hub
instance: hub_id
stages:
  s1:
    initial: true
    timers: [{id: h2.s1_to_s2, after: 1s, advances_to: s2}]
  s2:
    timers: [{id: h2.s2_to_s1, after: 1s, advances_to: s1}]
  closed: {terminal: true}
pins: {inputs: [hub.start, hub.bump, hub.close]}
`,
		"hub/entities.yaml": `hub:
  hub_id: text
  count: {type: integer, initial: 0}
  c1: {type: integer, initial: 0}
  c2: {type: integer, initial: 0}
`,
		"hub/nodes.yaml": `hub-node:
  execution_type: system_node
  subscribes_to: [hub.start, hub.bump, hub.close]
  event_handlers:
    hub.start:
      data_accumulation:
        writes:
          - {source_field: hub_id, target_field: hub_id}
          - {target_field: count, value: 0}
          - {target_field: c1, value: 0}
          - {target_field: c2, value: 0}
    hub.bump:
      data_accumulation:
        writes: [{target_field: count, value: entity.count + 1}]
      rules:
        - id: in_s1
          when: _entity.current_state == 's1'
          data_accumulation:
            writes: [{target_field: c1, value: entity.c1 + 1}]
        - id: in_s2
          else: true
          data_accumulation:
            writes: [{target_field: c2, value: entity.c2 + 1}]
    hub.close: {advances_to: closed}
`,
	}
	root := t.TempDir()
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), files[path])
		t.Logf("reconstructed_equivalent_H2 source=%s sha256=%x", path, sha256.Sum256([]byte(files[path])))
	}
	t.Log("equivalence_authority=https://github.com/division-sh/swarm/issues/2564#issuecomment-6001398406; archive_identity=NOT_UNCHANGED; bounded HTTP transport; independent submission rates are not durable admission rates; six keyed children in one root run; system-node handler writes; explicit hub.start counter initialization; two alternating nonterminal 1s stage timers; close is explicit")
	return root
}

func issue2564H2Harness(t *testing.T, backend, root string) (func(bool) (*channelOnboardingCrashServeProcess, servedControlProofRuntime), *os.File, *os.File) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	var db *sql.DB
	var config string
	if backend == "postgres" {
		dsn, connection, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		db, config = connection, writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
	} else {
		path := filepath.Join(t.TempDir(), "h2.sqlite")
		config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, path, channelOnboardingHostWorkspaceFields())
		var err error
		db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
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
	return func(cut bool) (*channelOnboardingCrashServeProcess, servedControlProofRuntime) {
		raw, err := json.Marshal(issue2564H2Child{Source: root, Config: config, Backend: backend, Cut: cut})
		if err != nil {
			t.Fatal(err)
		}
		process := startServedCrashProcess(t, "TestIssue2564H2ServeProcessHelper", []string{issue2564H2ChildEnv + "=" + string(raw), "TMPDIR=" + temporary}, armR, cutW)
		return process, servedControlProofRuntime{Endpoint: process.endpoint(t) + "/v1/rpc", DB: db, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, root)}
	}, armW, cutR
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

func issue2564H2Time(value any) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	if stamp, ok := value.(time.Time); ok {
		return stamp.UTC(), nil
	}
	if raw, ok := value.([]byte); ok {
		value = string(raw)
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999+00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999 -0700 MST"} {
		if stamp, err := time.Parse(layout, fmt.Sprint(value)); err == nil {
			return stamp.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("H2 timestamp %T %q", value, value)
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

func issue2564H2Read(ctx context.Context, rt servedControlProofRuntime, run string) (issue2564H2Snapshot, error) {
	snapshot := issue2564H2Snapshot{Hubs: map[string]issue2564H2Hub{}, Timers: map[string]issue2564H2Timer{}, Events: map[string]issue2564H2Event{}}
	options := &sql.TxOptions{ReadOnly: true}
	if rt.Backend == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	tx, err := rt.DB.BeginTx(ctx, options)
	if err != nil {
		return snapshot, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT s.entity_id,s.flow_instance,s.current_state,s.fields,s.revision,f.current_state,f.revision,f.config FROM entity_state s JOIN flow_instances f ON f.run_id=s.run_id AND f.entity_id=s.entity_id WHERE s.run_id=$1`, run)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var hub issue2564H2Hub
		var fields, config []byte
		var headerState string
		var headerRevision int64
		if err := rows.Scan(&hub.Entity, &hub.Instance, &hub.Stage, &fields, &hub.Revision, &headerState, &headerRevision, &config); err != nil {
			rows.Close()
			return snapshot, err
		}
		hub.ID, hub.Count, hub.C1, hub.C2, err = issue2564H2Counters(fields)
		if err != nil {
			rows.Close()
			return snapshot, err
		}
		if hub.Stage != headerState || hub.Revision != headerRevision || snapshot.Hubs[hub.ID].ID != "" {
			rows.Close()
			return snapshot, fmt.Errorf("H2 header/field identity disagreement for %+v", hub)
		}
		var persisted struct {
			History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
		}
		if err := json.Unmarshal(config, &persisted); err != nil {
			rows.Close()
			return snapshot, err
		}
		hub.History = persisted.History
		snapshot.Hubs[hub.ID] = hub
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return snapshot, err
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT timer_id,timer_name,entity_id,flow_instance,status,created_at,fire_at,fired_at FROM timers WHERE run_id=$1 AND task_type='workflow_timer'`, run)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var timer issue2564H2Timer
		var name string
		var created, due, fired any
		if err := rows.Scan(&timer.ID, &name, &timer.Entity, &timer.Instance, &timer.Status, &created, &due, &fired); err != nil {
			rows.Close()
			return snapshot, err
		}
		var valid bool
		timer.Ref, valid = timeridentity.ParseWorkflowTimerActivationTaskID(name)
		if !valid || timer.Ref.ActivationID != timer.ID {
			rows.Close()
			return snapshot, fmt.Errorf("H2 invalid exact timer identity: %+v", timer)
		}
		if timer.Created, err = issue2564H2Time(created); err != nil {
			rows.Close()
			return snapshot, err
		}
		if timer.Due, err = issue2564H2Time(due); err != nil {
			rows.Close()
			return snapshot, err
		}
		if timer.Fired, err = issue2564H2Time(fired); err != nil {
			rows.Close()
			return snapshot, err
		}
		snapshot.Timers[timer.ID] = timer
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return snapshot, err
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT e.event_id,e.task_id,e.flow_instance,COALESCE(r.outcome,''),COALESCE(r.reason_code,'') FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' WHERE e.run_id=$1 AND e.event_name='platform.stage_timer'`, run)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var event issue2564H2Event
		if err := rows.Scan(&event.ID, &event.Task, &event.Instance, &event.Outcome, &event.Reason); err != nil {
			rows.Close()
			return snapshot, err
		}
		var valid bool
		event.Occurrence, valid = timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.Task)
		if !valid || timeridentity.WorkflowTimerOccurrenceEventID(event.Occurrence) != event.ID {
			rows.Close()
			return snapshot, fmt.Errorf("H2 publication lacks exact occurrence identity: %+v", event)
		}
		snapshot.Events[event.ID] = event
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return snapshot, err
	}
	rows.Close()
	return snapshot, tx.Commit()
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

func issue2564H2WaitAccounting(t *testing.T, rt servedControlProofRuntime, run string, closed bool, keys map[string]string) issue2564H2Snapshot {
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
	t.Fatalf("exact H2 accounting never settled: %v\n%s", last, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, run))
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

func issue2564H2HTTPClient(t *testing.T, rt servedControlProofRuntime) *http.Client {
	t.Helper()
	limit := 64
	if rt.Backend == "postgres" {
		var capacity int
		if err := rt.DB.QueryRow(`SHOW max_connections`).Scan(&capacity); err != nil {
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

func issue2564H2PGSessionSampler(t *testing.T, ctx context.Context, rt servedControlProofRuntime, run string, count int, acknowledged *sync.Map) (func(string), func()) {
	t.Helper()
	if rt.Backend != "postgres" {
		return func(string) {}, func() {}
	}
	// Retain only the test's read connection, before pressure, so native session
	// attribution remains observable even when runtime connections are exhausted.
	conn, err := rt.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var actorKind, actorID string
	if err := conn.QueryRowContext(ctx, `SELECT actor_kind,actor_id FROM api_idempotency WHERE method='event.publish' AND idempotency_key='h2-start-1'`).Scan(&actorKind, &actorID); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	apiKeys := make([]string, count)
	for ordinal := range apiKeys {
		encoded, err := json.Marshal([]string{"event.publish", actorKind, actorID, fmt.Sprintf("h2-bump-%04d", ordinal)})
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		apiKeys[ordinal] = "swarm:api-idempotency:" + string(encoded)
	}
	// These read-only lock identities match the existing store owners' namespaces
	// and retainedAdvisoryLockProofSQL. They confer no execution/claim authority.
	const query = `
		WITH pipeline_keys AS MATERIALIZED (
			SELECT event_id,hashtext('swarm:pipeline-replay:' || event_id::text)::bigint AS key,
			       event_id::text = ANY($3::text[]) AS acknowledged
			FROM events WHERE run_id=$1::uuid
		), api_keys AS MATERIALIZED (
			SELECT hashtext(unnest($2::text[]))::bigint AS key
		), held AS MATERIALIZED (
			SELECT pid,classid::bigint AS classid,objid::bigint AS objid
			FROM pg_locks WHERE locktype='advisory' AND granted AND objsubid=1
			  AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		), pipeline AS MATERIALIZED (
			SELECT DISTINCT h.pid,k.event_id,k.acknowledged FROM held h JOIN pipeline_keys k
			  ON h.classid=CASE WHEN k.key<0 THEN 4294967295::bigint ELSE 0::bigint END
			 AND h.objid=(k.key & 4294967295::bigint)
		), api AS MATERIALIZED (
			SELECT DISTINCT h.pid FROM held h JOIN api_keys k
			  ON h.classid=CASE WHEN k.key<0 THEN 4294967295::bigint ELSE 0::bigint END
			 AND h.objid=(k.key & 4294967295::bigint)
		), sessions AS (
			SELECT a.pid,COALESCE(a.datname,'') AS database,COALESCE(a.application_name,'') AS application,
			       COALESCE(a.state,'') AS state,
			       CASE WHEN EXISTS(SELECT 1 FROM pipeline p WHERE p.pid=a.pid) THEN
			         CASE WHEN EXISTS(SELECT 1 FROM api k WHERE k.pid=a.pid) THEN 'pipeline+api' ELSE 'pipeline_only' END
			         ELSE CASE WHEN EXISTS(SELECT 1 FROM api k WHERE k.pid=a.pid) THEN 'api_only' ELSE 'unmapped' END END AS owner,
			       (SELECT COUNT(*) FROM pipeline p WHERE p.pid=a.pid) AS pipeline_keys,
			       (SELECT COUNT(*) FROM pipeline p WHERE p.pid=a.pid AND p.acknowledged) AS acknowledged_keys,
			       EXTRACT(EPOCH FROM (clock_timestamp()-a.state_change)) AS state_age
			FROM pg_stat_activity a WHERE a.backend_type='client backend' AND a.pid<>pg_backend_pid()
		)
		SELECT database,application,state,owner,COUNT(*),SUM(pipeline_keys),SUM(acknowledged_keys),COALESCE(MAX(state_age),0)
		FROM sessions GROUP BY database,application,state,owner ORDER BY database,application,state,owner`
	return func(label string) {
		var acked []string
		acknowledged.Range(func(event, _ any) bool { acked = append(acked, event.(string)); return true })
		sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rows, err := conn.QueryContext(sampleCtx, query, run, pq.Array(apiKeys), pq.Array(acked))
		if err != nil {
			t.Logf("H2 native_pg_sessions phase=%s acknowledged_HTTP=%d error=%v", label, len(acked), err)
			return
		}
		defer rows.Close()
		total, pipeline, api, ackedClaims := 0, 0, 0, 0
		for rows.Next() {
			var database, application, state, owner string
			var sessions, keys, acknowledgedKeys int
			var age float64
			if err := rows.Scan(&database, &application, &state, &owner, &sessions, &keys, &acknowledgedKeys, &age); err != nil {
				t.Logf("H2 native_pg_sessions phase=%s scan_error=%v", label, err)
				return
			}
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
		if err := rows.Err(); err != nil {
			t.Logf("H2 native_pg_sessions phase=%s rows_error=%v", label, err)
		}
		t.Logf("H2 native_pg_session_totals phase=%s client_sessions=%d pipeline_owner_sessions=%d api_owner_sessions=%d acknowledged_HTTP=%d still_held_ACKed_event_keys=%d sampler_backend_excluded=true", label, total, pipeline, api, len(acked), ackedClaims)
	}, func() { _ = conn.Close() }
}

func issue2564H2Bumps(t *testing.T, rt servedControlProofRuntime, seed servedEventPublishRPCResult, count, rate int) map[string]string {
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
				rows, err := rt.DB.QueryContext(ctx, `SELECT e.event_name,d.status,COUNT(*) FROM events e LEFT JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 GROUP BY e.event_name,d.status`, seed.RunID)
				if err != nil {
					t.Logf("H2 live response-queue diagnostic query: %v", err)
					continue
				}
				var progress []string
				for rows.Next() {
					var name string
					var status sql.NullString
					var n int
					if err := rows.Scan(&name, &status, &n); err != nil {
						progress = append(progress, err.Error())
						break
					}
					progress = append(progress, fmt.Sprintf("%s/%s=%d", name, status.String, n))
				}
				if err := rows.Err(); err != nil {
					progress = append(progress, err.Error())
				}
				rows.Close()
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
				var persisted, pending int
				_ = rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1`, seed.RunID).Scan(&persisted)
				_ = rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`, seed.RunID).Scan(&pending)
				t.Fatalf("H2 workload pacing deadline expired: issued=%d/%d completed=%d persisted_events=%d pending_deliveries=%d first_error=%v\n%s", ordinal, count, completed, persisted, pending, firstFailure, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
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
		t.Fatalf("H2 issued=%d accepted=%d input_rate=%.3f/s span=%s RPC_errors=%v\n%s", count, len(accepted), observed, last.Sub(first), failures, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
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

func issue2564H2CounterEvidence(t *testing.T, rt servedControlProofRuntime, run string, accepted map[string]string, snapshot issue2564H2Snapshot) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT entity_id,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT),COALESCE(CAST(caused_by_event AS TEXT),'') FROM entity_mutations WHERE run_id=$1 AND domain='authored_field' AND path IN ('count','c1','c2')`, run)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	chains, effects := map[string]map[int64]int64{}, map[string]map[string]int{}
	for rows.Next() {
		var entity, path, cause string
		var old, next sql.NullString
		if err := rows.Scan(&entity, &path, &old, &next, &cause); err != nil {
			t.Fatal(err)
		}
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
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

func issue2564H2Deliveries(t *testing.T, rt servedControlProofRuntime, run string, accepted map[string]string, userEvents int) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT d.event_id,d.status,d.retry_count,e.event_name,e.payload FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND d.subscriber_type='node'`, run)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	total, bumped := 0, map[string]bool{}
	for rows.Next() {
		var event, status, name string
		var retries int
		var payload []byte
		if err := rows.Scan(&event, &status, &retries, &name, &payload); err != nil {
			t.Fatal(err)
		}
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if total != userEvents || len(bumped) != len(accepted) {
		t.Fatalf("H2 real node delivery counts=%d/%d bumps=%d/%d", total, userEvents, len(bumped), len(accepted))
	}
	var dead, retried int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, run).Scan(&dead); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.outcome='retry_scheduled' OR a.outcome='dead_letter')`, run).Scan(&retried); err != nil {
		t.Fatal(err)
	}
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
						var pending int
						_ = rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id IS NOT NULL AND status IN ('pending','in_progress')`).Scan(&pending)
						t.Logf("H2 failure pending_deliveries=%d", pending)
						t.Logf("H2 first process raw output:\n%s", first.output.String())
					}
				})
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "h2-start-1"})
				for hub := 2; hub <= 6; hub++ {
					requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"hub_id": fmt.Sprintf("h%02d", hub)}, "idempotency_key": fmt.Sprintf("h2-start-%d", hub)})
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
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
				var constructions int
				if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM workflow_instance_initial_materializations WHERE run_id=$1`, seed.RunID).Scan(&constructions); err != nil || constructions != 7 {
					t.Fatalf("H2 requires one root plus six real keyed-child constructions: %d %v", constructions, err)
				}
				accepted := issue2564H2Bumps(t, rt, seed, workload.bumps, workload.rate)
				issue2564H2AdmissionEvidence(t, first.output.String(), accepted)
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
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
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
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
