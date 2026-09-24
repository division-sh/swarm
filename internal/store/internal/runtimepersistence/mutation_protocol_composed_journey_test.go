package runtimepersistence_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimeconfig "github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type mutationProtocolJourneyFixture struct {
	backend, root, repo, configPath, dsn string
	selection                            storebackend.Selection
	db                                   *sql.DB
}

type mutationProtocolJourneyRuntime struct {
	runtime    *swarmruntime.Runtime
	selected   *storeselected.Owner
	process    *worklifetime.Process
	capability startupownership.ProcessCapability
	ctx        context.Context
	closed     bool
}

func newMutationProtocolJourneyFixture(t *testing.T, backend string) mutationProtocolJourneyFixture {
	t.Helper()
	return newMutationProtocolJourneyFixtureWithRoot(t, backend, canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false))
}

func mutationProtocolFanOutRoot(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
	for _, edit := range []struct {
		file, old, replacement string
	}{
		{"schema.yaml", "      - {event: start.closed, source: external}\n", "      - {event: start.closed, source: external}\n      - {event: fanout.requested, source: external}\n"},
		{"schema.yaml", "events: [work.requested", "events: [fanout.child, work.requested"},
		{"nodes.yaml", "subscribes_to: [start.seeded, start.requested, start.closed]", "subscribes_to: [start.seeded, start.requested, start.closed, fanout.requested]"},
	} {
		path := filepath.Join(root, edit.file)
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(content), edit.old) != 1 {
			t.Fatalf("fan-out source replacement %q is not unique in %s", edit.old, path)
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(content), edit.old, edit.replacement, 1)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, addition := range []struct{ file, content string }{
		{"events.yaml", "fanout.requested:\n  items: '[text]'\nfanout.child:\n  value: text\n  swarm:\n    consumer: external\n"},
		{"nodes.yaml", "    fanout.requested:\n      fan_out:\n        items_from: payload.items\n        as: entry\n        identity: entry\n        max_items: 2\n        emit:\n          event: fanout.child\n          fields:\n            value: {cel: entry}\n"},
	} {
		path := filepath.Join(root, addition.file)
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString(addition.content); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func newMutationProtocolJourneyFixtureWithRoot(t *testing.T, backend, root string) mutationProtocolJourneyFixture {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	for _, key := range []string{"SWARM_CONFIG", "SWARM_STORE_BACKEND", "SWARM_SQLITE_PATH", "SWARM_API_TOKEN", "SWARM_API_TOKEN_FILE"} {
		t.Setenv(key, "")
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "journey.db")
	f := mutationProtocolJourneyFixture{
		backend: backend, root: root, repo: repo, configPath: filepath.Join(dir, "swarm.yaml"),
		selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: sqlitePath},
	}
	config := map[string]any{
		"runtime": map[string]any{"recovery_on_startup": false},
		"store":   map[string]any{"backend": backend, "sqlite": map[string]any{"path": sqlitePath}},
		"llm":     map[string]any{"backend": "anthropic", "session": map[string]any{"lock_ttl": "10s", "rotate_after_turns": 40, "rotate_on_parse_failures": 3}},
	}
	if backend == "postgres" {
		var cleanup func()
		f.dsn, f.db, cleanup = testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		connection, err := testpostgres.ParseConnection(f.dsn)
		if err != nil {
			t.Fatal(err)
		}
		p := connection.Parameters()
		t.Setenv("MUTATION_JOURNEY_DB_PASSWORD", p.Password)
		config["database"] = map[string]any{"host": p.Host, "port": p.Port, "name": p.Database, "user": p.User, "password_env": "MUTATION_JOURNEY_DB_PASSWORD", "sslmode": p.SSLMode}
		f.selection = storebackend.Selection{Backend: storebackend.BackendPostgres}
	} else {
		var err error
		f.db, err = sql.Open("sqlite", sqlitePath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.db.Close() })
	}
	raw, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f mutationProtocolJourneyFixture) start(t *testing.T, bootstrap bool) *mutationProtocolJourneyRuntime {
	t.Helper()
	selected, err := storeselected.OpenRuntime(context.Background(), storeselected.RuntimeRequest{Selection: f.selection, PostgresDSN: f.dsn})
	if err != nil {
		t.Fatal(err)
	}
	s := &mutationProtocolJourneyRuntime{selected: selected, process: worklifetime.NewProcess()}
	t.Cleanup(func() { s.close(t) })
	module, bundle, err := cliapp.NewSwarmWorkflowModule(f.repo, f.root, filepath.Join(f.repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	report := bootverify.Run(context.Background(), module.SemanticSource(), bootverify.Options{})
	if len(report.Errors()) != 0 || len(report.Warnings()) != 0 {
		t.Fatalf("ordinary journey boot verification: errors=%+v warnings=%+v", report.Errors(), report.Warnings())
	}
	plans, err := cliapp.StateStoreSchemaPlans(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.Schema().BootstrapSchema(context.Background(), store.SchemaBootstrapRequest{
		PlatformPlans: plans.Platform, StatePlans: plans.State,
		Origin: store.RuntimeStoreOrigin{SwarmVersion: "mutation-protocol-journey", PlatformVersion: bundle.Platform.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	s.ctx = runtimecorrelation.WithRuntimeInstanceID(context.Background(), runtimeID)
	s.ctx = runtimecorrelation.WithSourceArtifactFact(s.ctx, fact)
	s.ctx = runtimeauthoractivity.WithScope(s.ctx, runtimeauthoractivity.BundleScope(runtimeID, fact.BundleHash()))
	if bootstrap {
		storetest.RequireBundleDataCatalog(t, s.ctx, selected.SourceArtifactWriter(), bundle)
	}
	cfg, err := runtimeconfig.Load(f.configPath)
	if err != nil {
		t.Fatal(err)
	}
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(bundle.PackInventory, bundle.Platform.Platform.Version)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(filepath.Dir(f.configPath), "provider-credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	deps := selected.RuntimeDeps()
	deps.Config = cfg
	deps.Options = swarmruntime.RuntimeOptions{ExecutionPosture: executionposture.Live, WorkflowModule: module, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID, ProcessWorkOwner: s.process, ProviderTriggerCatalog: catalog, ProviderCredentials: credentials}
	s.runtime, err = swarmruntime.NewRuntime(s.ctx, deps)
	if err != nil {
		t.Fatal(err)
	}
	s.capability, err = selected.StartupOwnership().AcquireProcessCapability(s.ctx, startupownership.AcquireRequest{OwnerID: "mutation-protocol-journey", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID})
	if err != nil {
		t.Fatal(err)
	}
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	agents, err := s.runtime.Manager.CompileStaticTopologyDesiredAgents(module.SemanticSource(), coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, agents)
	if err != nil {
		t.Fatal(err)
	}
	current, exists, err := s.capability.CurrentSourceSet(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		if !reflect.DeepEqual(current, plan) {
			t.Fatalf("restarted source set differs from persisted authority: current=%+v planned=%+v", current, plan)
		}
	} else if bootstrap {
		if _, err := s.capability.InstallCompleteSourceSet(s.ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("restart lost committed source-set authority")
	}
	grant, err := s.capability.IssueGenerationGrant(s.ctx, startupownership.GrantRequest{BundleHash: fact.BundleHash(), RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.runtime.InstallStartupGrant(grant); err != nil {
		t.Fatal(err)
	}
	if err := s.runtime.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatal(err)
	}
	if err := s.runtime.Start(s.ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *mutationProtocolJourneyRuntime) close(t *testing.T) {
	t.Helper()
	if s.closed {
		return
	}
	s.closed = true
	if s.runtime != nil {
		if err := s.runtime.Shutdown(); err != nil {
			t.Error(err)
		}
	}
	if s.process != nil {
		s.process.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := s.process.Join(ctx); err != nil {
			t.Error(err)
		}
		cancel()
	}
	if s.capability != nil {
		if err := s.capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	}
	if s.selected != nil {
		if err := s.selected.CloseUnactivated(); err != nil {
			t.Error(err)
		}
	}
}

type mutationProtocolJourneyFacts struct {
	consumerFields, consumerState, eventID, deliveryID, deliveryStatus               string
	consumerRevision, claimVersion, headRevision, activitySequence                   int64
	eventFactRevision, deliveryFactRevision                                          int64
	events, readyEvents, deliveries, readyDeliveries, receipts, activities, factRows int
}

func readMutationProtocolJourneyFacts(t *testing.T, db *sql.DB, runID string) mutationProtocolJourneyFacts {
	t.Helper()
	var f mutationProtocolJourneyFacts
	if err := db.QueryRow(`SELECT CAST(fields AS TEXT),current_state,revision FROM entity_state WHERE run_id=$1 AND flow_instance='consumer'`, runID).Scan(&f.consumerFields, &f.consumerState, &f.consumerRevision); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, runID).Scan(&f.eventID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT delivery_id,status,claim_version FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, f.eventID).Scan(&f.deliveryID, &f.deliveryStatus, &f.claimVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, runID).Scan(&f.headRevision); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		query string
		args  []any
		out   *int
	}{
		{`SELECT COUNT(*) FROM events WHERE run_id=$1`, []any{runID}, &f.events},
		{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, []any{runID}, &f.readyEvents},
		{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, []any{runID}, &f.deliveries},
		{`SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, []any{f.eventID}, &f.readyDeliveries},
		{`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, []any{f.eventID}, &f.receipts},
		{`SELECT COUNT(*) FROM author_activity_occurrences WHERE run_id=$1 AND kind='event.emitted' AND source_owner='events' AND source_identity=$2`, []any{runID, f.eventID}, &f.activities},
		{`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1`, []any{runID}, &f.factRows},
	} {
		if err := db.QueryRow(item.query, item.args...).Scan(item.out); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.QueryRow(`SELECT sequence FROM author_activity_occurrences WHERE run_id=$1 AND kind='event.emitted' AND source_owner='events' AND source_identity=$2`, runID, f.eventID).Scan(&f.activitySequence); err != nil {
		t.Fatal(err)
	}
	for _, fact := range []struct {
		family, key string
		revision    *int64
	}{{"events", f.eventID, &f.eventFactRevision}, {"event_deliveries", f.deliveryID, &f.deliveryFactRevision}} {
		if err := db.QueryRow(`SELECT COALESCE(MAX(revision),0) FROM run_fork_fact_revisions WHERE run_id=$1 AND family=$2 AND fact_key=$3 AND present=TRUE`, runID, fact.family, fact.key).Scan(fact.revision); err != nil {
			t.Fatal(err)
		}
		if *fact.revision == 0 {
			t.Fatalf("missing historical %s fact for %s", fact.family, fact.key)
		}
	}
	return f
}

func waitForMutationProtocolSeed(t *testing.T, db *sql.DB, runID string) int64 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var fields string
		var revision int64
		var delivered int
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.event_name='receiver.seeded' AND d.subscriber_type='node' AND d.status='delivered' AND d.claim_version>0`, runID).Scan(&delivered); err != nil {
			t.Fatal(err)
		}
		err := db.QueryRow(`SELECT CAST(fields AS TEXT),revision FROM entity_state WHERE run_id=$1 AND flow_instance='consumer'`, runID).Scan(&fields, &revision)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if err == nil {
			var values map[string]any
			if err := json.Unmarshal([]byte(fields), &values); err != nil {
				t.Fatal(err)
			}
			if delivered == 1 && values["marker"] == "consumer-owned" && values["processed_token"] == "seeded" {
				return revision
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("seed receiver did not finish its claimed ordinary delivery")
	return 0
}

func waitForMutationProtocolJourney(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var emitted, delivered, updated int
		if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, runID).Scan(&emitted); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.event_name='producer/work.ready' AND d.subscriber_type='node' AND d.status='delivered' AND d.claim_version>0`, runID).Scan(&delivered); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND flow_instance='consumer' AND CAST(fields AS TEXT) LIKE '%journey-token%'`, runID).Scan(&updated); err != nil {
			t.Fatal(err)
		}
		if emitted == 1 && delivered == 1 && updated == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("claimed ordinary receiver did not emit, settle and mutate state")
}

func TestMutationProtocolComposedOrdinaryJourneyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newMutationProtocolJourneyFixture(t, backend)
			first := fixture.start(t, true)
			runID := uuid.NewString()
			seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "mutation-protocol-journey", "", []byte(`{"token":"seeded"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := first.runtime.Bus.PublishAndWait(first.ctx, seed); err != nil {
				t.Fatal(err)
			}
			seedRevision := waitForMutationProtocolSeed(t, fixture.db, runID)
			request := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "mutation-protocol-journey", "", []byte(`{"token":"journey-token"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := first.runtime.Bus.PublishAndWait(first.ctx, request); err != nil {
				t.Fatal(err)
			}
			waitForMutationProtocolJourney(t, fixture.db, runID)
			before := readMutationProtocolJourneyFacts(t, fixture.db, runID)
			var fields map[string]any
			if err := json.Unmarshal([]byte(before.consumerFields), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["marker"] != "consumer-owned" || fields["processed_token"] != "journey-token" || before.consumerState != "active" || before.consumerRevision != seedRevision+1 {
				t.Fatalf("ordinary state transition = %+v fields=%v", before, fields)
			}
			if before.eventID == "" || before.deliveryID == "" || before.readyEvents != 1 || before.readyDeliveries != 1 || before.deliveryStatus != "delivered" || before.claimVersion < 1 || before.receipts != 1 || before.activities != 1 || before.activitySequence < 1 || before.eventFactRevision < 1 || before.deliveryFactRevision < before.eventFactRevision || before.headRevision < before.deliveryFactRevision || before.factRows < 2 {
				t.Fatalf("ordinary publication, claim, story or history incomplete: %+v", before)
			}
			first.close(t)
			second := fixture.start(t, false)
			reopened := readMutationProtocolJourneyFacts(t, fixture.db, runID)
			if !reflect.DeepEqual(before, reopened) {
				t.Fatalf("restart changed committed ordinary facts: before=%+v after=%+v", before, reopened)
			}
			if err := second.runtime.Bus.PublishAndWait(second.ctx, request); err != nil {
				t.Fatalf("restart duplicate publish: %v", err)
			}
			after := readMutationProtocolJourneyFacts(t, fixture.db, runID)
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("restart duplicate made a second durable effect: before=%+v after=%+v", before, after)
			}
		})
	}
}

type mutationProtocolHistoryRow struct {
	revision    int64
	family, key string
	present     bool
	fact        string
}

type mutationProtocolHistorySnapshot struct {
	head, revisions int64
	facts           []mutationProtocolHistoryRow
}

func readMutationProtocolHistory(t *testing.T, db *sql.DB, runID string) mutationProtocolHistorySnapshot {
	t.Helper()
	var snapshot mutationProtocolHistorySnapshot
	if err := db.QueryRow(`SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1`, runID).Scan(&snapshot.head); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1`, runID).Scan(&snapshot.revisions); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT revision,family,fact_key,present,COALESCE(CAST(fact AS TEXT),'') FROM run_fork_fact_revisions WHERE run_id=$1 ORDER BY revision,family,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var fact mutationProtocolHistoryRow
		if err := rows.Scan(&fact.revision, &fact.family, &fact.key, &fact.present, &fact.fact); err != nil {
			t.Fatal(err)
		}
		snapshot.facts = append(snapshot.facts, fact)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func waitForMutationProtocolFanOutCut(t *testing.T, db *sql.DB, runID, parentID string) [2]string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var closed, children, receipts int
		if err := db.QueryRow(`SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND status='closed' AND cursor=cardinality AND cardinality=2`, runID).Scan(&closed); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND event_name='fanout.child'`, runID, parentID).Scan(&children); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND e.source_event_id=$2 AND e.event_name='fanout.child' AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, runID, parentID).Scan(&receipts); err != nil {
			t.Fatal(err)
		}
		if closed == 1 && children == 2 && receipts == 2 {
			rows, err := db.Query(`SELECT event_id FROM events WHERE run_id=$1 AND source_event_id=$2 AND event_name='fanout.child' ORDER BY event_id`, runID, parentID)
			if err != nil {
				t.Fatal(err)
			}
			var ids [2]string
			for i := range ids {
				if !rows.Next() || rows.Scan(&ids[i]) != nil {
					rows.Close()
					t.Fatal("cannot read exact grouped child events")
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			return ids
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("real fan-out group did not commit and settle its two child events")
	return [2]string{}
}

func installMutationProtocolLateRollback(t *testing.T, db *sql.DB, backend, runID, laterEventID string) {
	t.Helper()
	name := "mutation_protocol_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	condition := fmt.Sprintf("NEW.run_id='%s' AND EXISTS (SELECT 1 FROM events WHERE event_id='%s')", runID, laterEventID)
	query := fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON run_fork_revisions WHEN %s BEGIN SELECT RAISE(ABORT,'mutation_protocol_late_rollback'); END", name, condition)
	if backend == "postgres" {
		function := fmt.Sprintf("CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF %s THEN RAISE EXCEPTION 'mutation_protocol_late_rollback'; END IF; RETURN NEW; END $$", name, condition)
		if _, err := db.Exec(function); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := db.Exec("DROP FUNCTION IF EXISTS " + name + "()"); err != nil {
				t.Error(err)
			}
		})
		query = fmt.Sprintf("CREATE TRIGGER %s BEFORE INSERT ON run_fork_revisions FOR EACH ROW EXECUTE FUNCTION %s()", name, name)
	}
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		query := "DROP TRIGGER IF EXISTS " + name
		if backend == "postgres" {
			query += " ON run_fork_revisions"
		}
		if _, err := db.Exec(query); err != nil {
			t.Error(err)
		}
	})
}

func TestMutationProtocolFanOutCutSurvivesLaterRollbackAndForkReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := mutationProtocolFanOutRoot(t)
			fixture := newMutationProtocolJourneyFixtureWithRoot(t, backend, root)
			served := fixture.start(t, true)
			runID := uuid.NewString()
			second := eventtest.ExistingRunRootIngress(uuid.NewString(), "start.requested", "mutation-protocol-journey", "", []byte(`{"token":"rollback-token"}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			installMutationProtocolLateRollback(t, fixture.db, backend, runID, second.ID())
			seed := eventtest.RunCreatingRootIngress(uuid.NewString(), "start.seeded", "mutation-protocol-journey", "", []byte(`{"token":"seeded"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := served.runtime.Bus.PublishAndWait(served.ctx, seed); err != nil {
				t.Fatal(err)
			}
			waitForMutationProtocolSeed(t, fixture.db, runID)
			first := eventtest.ExistingRunRootIngress(uuid.NewString(), "fanout.requested", "mutation-protocol-journey", "", []byte(`{"items":["alpha","beta"]}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := served.runtime.Bus.PublishAndWait(served.ctx, first); err != nil {
				t.Fatal(err)
			}
			children := waitForMutationProtocolFanOutCut(t, fixture.db, runID, first.ID())
			history := readMutationProtocolHistory(t, fixture.db, runID)
			var childCut int64
			for _, child := range children {
				found := false
				for _, fact := range history.facts {
					if fact.family == "events" && fact.key == child && fact.present {
						if childCut != 0 && fact.revision != childCut {
							t.Fatalf("grouped child event cuts differ: %d and %d", childCut, fact.revision)
						}
						childCut = fact.revision
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("missing grouped child history: %s", child)
				}
			}
			receiptCuts := map[string]int64{}
			for _, fact := range history.facts {
				if fact.family != "event_receipts" || !fact.present {
					continue
				}
				var receipt struct {
					EventID        string `json:"event_id"`
					SubscriberType string `json:"subscriber_type"`
					SubscriberID   string `json:"subscriber_id"`
					Outcome        string `json:"outcome"`
				}
				if err := json.Unmarshal([]byte(fact.fact), &receipt); err != nil {
					t.Fatal(err)
				}
				if receipt.SubscriberType == "platform" && receipt.SubscriberID == "pipeline" && receipt.Outcome == "success" {
					receiptCuts[receipt.EventID] = fact.revision
				}
			}
			if receiptCuts[children[0]] <= childCut || receiptCuts[children[0]] != receiptCuts[children[1]] {
				t.Fatalf("grouped pipeline receipts have different or premature cuts: child=%d receipts=%v", childCut, receiptCuts)
			}
			fork, ok := served.selected.RunFork()
			if !ok {
				t.Fatal("selected run-fork reader unavailable")
			}
			request := runfork.RunForkPlanRequest{SourceRunID: runID, At: children[0]}
			beforePlan, err := fork.Plan(served.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if beforePlan.ForkPoint.Revision != childCut || beforePlan.ForkPoint.EventID != children[0] {
				t.Fatalf("grouped child fork point=%+v want revision %d", beforePlan.ForkPoint, childCut)
			}
			historical, ok := beforePlan.HistoricalEventIDs(childCut)
			if !ok {
				t.Fatal("fork planner did not expose the grouped child cut")
			}
			for _, child := range children {
				found := false
				for _, eventID := range historical {
					if eventID == child {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("fork planner omitted grouped child %s at revision %d: %v", child, childCut, historical)
				}
			}
			if err := served.runtime.Bus.PublishAndWait(served.ctx, second); err == nil || !strings.Contains(err.Error(), "mutation_protocol_late_rollback") {
				t.Fatalf("later mutation did not roll back at revision finalization: %v", err)
			}
			if after := readMutationProtocolHistory(t, fixture.db, runID); !reflect.DeepEqual(history, after) {
				t.Fatalf("later rollback changed earlier grouped cut: before=%+v after=%+v", history, after)
			}
			var laterEvents int
			if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1`, second.ID()).Scan(&laterEvents); err != nil || laterEvents != 0 {
				t.Fatalf("rolled-back later event persisted: count=%d err=%v", laterEvents, err)
			}
			afterPlan, err := fork.Plan(served.ctx, request)
			if err != nil || !reflect.DeepEqual(beforePlan, afterPlan) {
				t.Fatalf("fork readback changed after later rollback: before=%+v after=%+v err=%v", beforePlan, afterPlan, err)
			}
		})
	}
}
