package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	"github.com/google/uuid"
)

type eventRecordContractStore interface {
	semanticEventFixtureStore
	diagnosticRuntimeLogFixtureStore
	CommitSelectedForkEvent(context.Context, CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error)
}

func TestEventRecordExactPersistenceParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(eventRecordContractStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 18, 16, 0, 0, 123456000, time.UTC)
			runID := uuid.NewString()
			root := eventtest.RootIngress(uuid.NewString(), "contract.root", "gateway", "root-task", []byte(`{"root":true}`), 1, runID, "", events.EventEnvelope{}, now)
			if err := commitSemanticEventFixture(ctx, store, root); err != nil {
				t.Fatalf("commit root event: %v", err)
			}
			reference, err := events.NewOperatorReferenceProvenance(root.ID())
			if err != nil {
				t.Fatal(err)
			}
			lineage := events.EventLineage{RunID: runID, ParentEventID: root.ID(), TaskID: "child-task", ExecutionMode: executionmode.Live}
			eventsToCommit := []events.Event{
				eventtest.OperatorInjected(uuid.NewString(), "contract.operator", "operator", "operator-task", []byte(`{"operator":true}`), 0, runID, &reference, events.EventEnvelope{}, now.Add(time.Microsecond)),
				eventtest.ChildWithLineage(uuid.NewString(), "contract.child", "child-agent", "child-task", []byte(`{"child":true}`), 2, lineage, events.EventEnvelope{}, now.Add(2*time.Microsecond)),
				eventtest.Replay(uuid.NewString(), "contract.replay", "replay-agent", "child-task", []byte(`{"replay":true}`), 3, lineage, events.EventEnvelope{}, now.Add(3*time.Microsecond)),
				eventtest.RuntimeControl(uuid.NewString(), "contract.control", "runtime", "", []byte(`{"control":true}`), 0, runID, root.ID(), events.EventEnvelope{}, now.Add(4*time.Microsecond)),
				eventtest.RuntimeDiagnostic(uuid.NewString(), "contract.diagnostic", "runtime", "", []byte(`{"diagnostic":true}`), 0, runID, root.ID(), events.EventEnvelope{}, now.Add(5*time.Microsecond)),
			}
			for _, event := range eventsToCommit {
				if err := commitSemanticEventFixture(ctx, store, event); err != nil {
					t.Fatalf("commit %s event: %v", event.AdmissionClass(), err)
				}
				assertExactEventRecord(t, ctx, fixture, event)
			}

			diagnostic := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", []byte(`{"message":"proof"}`), 0, runID, "", events.EventEnvelope{}, now.Add(6*time.Microsecond))
			if err := commitDiagnosticRuntimeLogFixture(ctx, store, diagnostic); err != nil {
				t.Fatalf("commit diagnostic-direct event: %v", err)
			}
			assertExactEventRecord(t, ctx, fixture, diagnostic)

			forkRunID := uuid.NewString()
			forkRoot := eventtest.RootIngress(
				uuid.NewString(), "contract.fork_root", "fixture", "", []byte(`{}`), 0,
				forkRunID, "", events.EventEnvelope{}, now.Add(6*time.Microsecond),
			)
			if err := commitSemanticEventFixture(ctx, store, forkRoot); err != nil {
				t.Fatalf("commit selected-fork run root: %v", err)
			}
			selectedLineage, err := events.NewSelectedForkLineage(forkRunID, runID, root.ID(), "selection:contract-proof", "fork-task", executionmode.Live)
			if err != nil {
				t.Fatal(err)
			}
			selected := eventtest.SelectedForkReplay(uuid.NewString(), "contract.selected_fork", eventtest.Producer(events.EventProducerNode, "selected-node"), "fork-task", []byte(`{"fork":true}`), 0, selectedLineage, events.EventEnvelope{}, now.Add(7*time.Microsecond))
			admitted, err := events.AdmitForPersistence(selected, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := store.CommitSelectedForkEvent(ctx, CommitSelectedForkEventRequest{
				Commit: runtimebus.CommitPublishRequest{Event: admitted, ReplayScope: runtimereplayclaim.CommittedReplayScopeDirect},
				Lineage: RunForkSelectedContractExecutionLineage{
					ForkRunID: forkRunID, SourceRunID: runID,
					SourceEventID: root.ID(), ForkEventID: selected.ID(), EventName: string(selected.Type()),
					SelectionAuthority: selectedLineage.AuthorityStamp(), CreatedAt: selected.CreatedAt(),
				},
			})
			if err != nil || outcome != runtimebus.EventAppendInserted {
				t.Fatalf("commit selected-fork event: outcome=%v err=%v", outcome, err)
			}
			assertExactEventRecord(t, ctx, fixture, selected)
			assertExactEventRecord(t, ctx, fixture, root)
		})
	}
}

func TestEventRecordEveryFieldDuplicateParity(t *testing.T) {
	baseEvent := eventtest.RootIngress(uuid.NewString(), "duplicate.base", "gateway", "task", []byte(`{"value":1}`), 2, uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 18, 17, 0, 0, 0, time.UTC))
	admitted, err := events.AdmitForPersistence(baseEvent, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	base, err := eventrecord.FromAdmitted(admitted)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name   string
		mutate func(*persistedEventIdentity)
	}{
		{"class", func(r *persistedEventIdentity) { r.Class = events.EventAdmissionRuntimeControl }},
		{"run_id", func(r *persistedEventIdentity) { r.RunID = uuid.NewString() }},
		{"event_name", func(r *persistedEventIdentity) { r.EventName = "duplicate.changed" }},
		{"task_id", func(r *persistedEventIdentity) { r.TaskID = "changed" }},
		{"entity_id", func(r *persistedEventIdentity) { r.EntityID = uuid.NewString() }},
		{"flow_instance", func(r *persistedEventIdentity) { r.FlowInstance = "flow/changed" }},
		{"scope", func(r *persistedEventIdentity) { r.Scope = events.EventScopeFlow }},
		{"payload", func(r *persistedEventIdentity) { r.Payload = []byte(`{"value":2}`) }},
		{"execution_mode", func(r *persistedEventIdentity) { r.ExecutionMode = executionmode.Mock }},
		{"chain_depth", func(r *persistedEventIdentity) { r.ChainDepth++ }},
		{"produced_by", func(r *persistedEventIdentity) { r.ProducedBy = "other" }},
		{"produced_by_type", func(r *persistedEventIdentity) { r.ProducedByType = events.EventProducerAgent }},
		{"source_event_id", func(r *persistedEventIdentity) { r.SourceEventID = uuid.NewString() }},
		{"created_at", func(r *persistedEventIdentity) { r.CreatedAt = r.CreatedAt.Add(time.Microsecond) }},
		{"routing_source_kind", func(r *persistedEventIdentity) { r.RoutingSourceKind = events.RoutingSourceRuntimeInstance }},
		{"routing_source_authority", func(r *persistedEventIdentity) { r.RoutingSourceAuthority = "changed" }},
		{"source_route", func(r *persistedEventIdentity) { r.SourceRoute = []byte(`{"flow_id":"changed"}`) }},
		{"target_route", func(r *persistedEventIdentity) { r.TargetRoute = []byte(`{"flow_id":"changed"}`) }},
		{"target_set", func(r *persistedEventIdentity) { r.TargetSet = []byte(`[{"flow_id":"changed"}]`) }},
		{"operator_reference_event_id", func(r *persistedEventIdentity) { r.OperatorReferencedEventID = uuid.NewString() }},
		{"selected_fork_source_run_id", func(r *persistedEventIdentity) { r.SelectedForkSourceRunID = uuid.NewString() }},
		{"selected_fork_source_event_id", func(r *persistedEventIdentity) { r.SelectedForkSourceEventID = uuid.NewString() }},
		{"selected_fork_authority", func(r *persistedEventIdentity) { r.SelectedForkAuthorityStamp = "changed" }},
		{"selected_fork_lineage_owner_count", func(r *persistedEventIdentity) { r.SelectedForkLineageOwners = 1 }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := base.Clone()
			mutation.mutate(&changed)
			if changed.Equal(base) {
				t.Fatal("changed record remained equal")
			}
			if duplicate, err := resolveExistingEventIdentity(base.EventID, base, changed, true); duplicate || !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("duplicate=%v err=%v, want identity conflict", duplicate, err)
			}
		})
	}

	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(semanticEventFixtureStore)
			ctx := testAuthorActivityContext()
			outcome, err := commitSemanticEventFixtureOutcome(ctx, store, baseEvent, nil, "direct")
			if err != nil || outcome != runtimebus.EventAppendInserted {
				t.Fatalf("initial commit: outcome=%v err=%v", outcome, err)
			}
			outcome, err = commitSemanticEventFixtureOutcome(ctx, store, baseEvent, nil, "direct")
			if err != nil || outcome != runtimebus.EventAppendExactDuplicate {
				t.Fatalf("exact duplicate: outcome=%v err=%v", outcome, err)
			}
			conflict := eventtest.RootIngress(baseEvent.ID(), baseEvent.Type(), baseEvent.SourceAgent(), baseEvent.TaskID(), []byte(`{"value":2}`), baseEvent.ChainDepth(), baseEvent.RunID(), "", baseEvent.NormalizedEnvelope(), baseEvent.CreatedAt())
			if _, err := commitSemanticEventFixtureOutcome(ctx, store, conflict, nil, "direct"); !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("conflicting duplicate error = %v", err)
			}
			nestedID := uuid.NewString()
			nested := eventtest.RootIngress(nestedID, "duplicate.nested", "gateway", "task", []byte(`{"nested":{"a":null}}`), 0, uuid.NewString(), "", events.EventEnvelope{}, baseEvent.CreatedAt())
			if outcome, err := commitSemanticEventFixtureOutcome(ctx, store, nested, nil, "direct"); err != nil || outcome != runtimebus.EventAppendInserted {
				t.Fatalf("nested initial commit: outcome=%v err=%v", outcome, err)
			}
			nestedConflict := eventtest.RootIngress(nestedID, nested.Type(), nested.SourceAgent(), nested.TaskID(), []byte(`{"nested":{"b":null}}`), 0, nested.RunID(), "", events.EventEnvelope{}, nested.CreatedAt())
			if _, err := commitSemanticEventFixtureOutcome(ctx, store, nestedConflict, nil, "direct"); !errors.Is(err, ErrEventIdentityConflict) {
				t.Fatalf("nested null-key duplicate error = %v", err)
			}
		})
	}
}

func TestEventRecordBatchHydrationContractParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(semanticEventFixtureStore)
			ctx := testAuthorActivityContext()
			batchSize := eventrecordsqlite.HydrationBatchSize()
			load := func(ctx context.Context, q eventReadQueryer, ids []string) ([]eventrecord.Record, error) {
				return eventrecordsqlite.LoadMany(ctx, q, ids)
			}
			if fixture.dialect == "postgres" {
				batchSize = eventrecordpostgres.HydrationBatchSize()
				load = func(ctx context.Context, q eventReadQueryer, ids []string) ([]eventrecord.Record, error) {
					return eventrecordpostgres.LoadMany(ctx, q, ids)
				}
			}

			runID := uuid.NewString()
			createdAt := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
			ids := make([]string, batchSize*2+3)
			for index := range ids {
				ids[index] = uuid.NewString()
				event := eventtest.RootIngress(
					ids[index], "batch.contract", "gateway", fmt.Sprintf("task-%d", index),
					[]byte(fmt.Sprintf(`{"index":%d}`, index)), 0, runID, "", events.EventEnvelope{}, createdAt.Add(time.Duration(index)*time.Microsecond),
				)
				if err := commitSemanticEventFixture(ctx, store, event); err != nil {
					t.Fatalf("commit event %d: %v", index, err)
				}
			}

			for _, size := range []int{0, 1, batchSize, batchSize + 1, batchSize*2 + 3} {
				t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
					requested := append([]string(nil), ids[:size]...)
					for left, right := 0, len(requested)-1; left < right; left, right = left+1, right-1 {
						requested[left], requested[right] = requested[right], requested[left]
					}
					queryer := &countedEventRecordQueryer{db: fixture.db}
					records, err := load(ctx, queryer, requested)
					if err != nil {
						t.Fatalf("load %d records: %v", size, err)
					}
					wantQueries := 0
					if size > 0 {
						wantQueries = (size + batchSize - 1) / batchSize
					}
					if queryer.queries != wantQueries {
						t.Fatalf("queries = %d, want %d", queryer.queries, wantQueries)
					}
					if len(records) != len(requested) {
						t.Fatalf("records = %d, want %d", len(records), len(requested))
					}
					for index := range requested {
						if records[index].EventID != requested[index] {
							t.Fatalf("record %d = %s, want %s", index, records[index].EventID, requested[index])
						}
					}
				})
			}

			t.Run("duplicate_id", func(t *testing.T) {
				queryer := &countedEventRecordQueryer{db: fixture.db}
				if records, err := load(ctx, queryer, []string{ids[0], ids[0]}); err == nil || records != nil {
					t.Fatalf("records=%v err=%v, want all-or-nothing duplicate rejection", records, err)
				}
				if queryer.queries != 0 {
					t.Fatalf("duplicate input executed %d queries", queryer.queries)
				}
			})

			t.Run("missing_id", func(t *testing.T) {
				queryer := &countedEventRecordQueryer{db: fixture.db}
				records, err := load(ctx, queryer, []string{ids[0], uuid.NewString()})
				if !errors.Is(err, eventrecord.ErrMissing) || records != nil {
					t.Fatalf("records=%v err=%v, want typed all-or-nothing missing failure", records, err)
				}
			})

			t.Run("cancelled", func(t *testing.T) {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				queryer := &countedEventRecordQueryer{db: fixture.db}
				records, err := load(cancelled, queryer, []string{ids[0]})
				if !errors.Is(err, context.Canceled) || records != nil {
					t.Fatalf("records=%v err=%v, want cancellation", records, err)
				}
			})

			t.Run("corrupt_record", func(t *testing.T) {
				eventtestsql.CorruptEventStore(t, ctx, fixture.db, fixture.dialect, eventtestsql.EventCorruptionClaim{
					Invariant: "store.event_record.canonical_readback",
					Reason:    "prove canonical batch hydration rejects a malformed durable envelope without a partial result",
				}, `UPDATE events SET target_route = ? WHERE event_id = ?`, `UPDATE events SET target_route = $1::jsonb WHERE event_id = $2::uuid`, `"bad"`, ids[len(ids)-1])
				queryer := &countedEventRecordQueryer{db: fixture.db}
				records, err := load(ctx, queryer, []string{ids[0], ids[len(ids)-1]})
				if !errors.Is(err, eventrecord.ErrCorrupt) || records != nil {
					t.Fatalf("records=%v err=%v, want typed all-or-nothing corruption failure", records, err)
				}
			})
		})
	}
}

func TestEventRecordDecoderRejectsMalformedDurableFactsParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			event := eventtest.RootIngress(
				uuid.NewString(), "decoder.contract", "gateway", "task", []byte(`{"ok":true}`), 0,
				uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 18, 18, 30, 0, 0, time.UTC),
			)
			if err := commitSemanticEventFixture(ctx, fixture.store.(semanticEventFixtureStore), event); err != nil {
				t.Fatalf("commit event: %v", err)
			}
			base, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
			if err != nil || !found {
				t.Fatalf("load event record: found=%v err=%v", found, err)
			}
			for _, mutation := range []struct {
				name   string
				mutate func(*persistedEventIdentity)
			}{
				{"invalid_class", func(record *persistedEventIdentity) { record.Class = "projection" }},
				{"invalid_event_id", func(record *persistedEventIdentity) { record.EventID = "not-a-uuid" }},
				{"missing_producer", func(record *persistedEventIdentity) { record.ProducedBy = "" }},
				{"child_without_parent", func(record *persistedEventIdentity) { record.Class = events.EventAdmissionChild }},
				{"root_with_operator_provenance", func(record *persistedEventIdentity) { record.OperatorReferencedEventID = uuid.NewString() }},
				{"runtime_source_without_route", func(record *persistedEventIdentity) { record.RoutingSourceKind = events.RoutingSourceRuntimeInstance }},
				{"selected_fork_without_lineage", func(record *persistedEventIdentity) { record.Class = events.EventAdmissionSelectedForkReplay }},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					malformed := base.Clone()
					mutation.mutate(&malformed)
					if _, err := malformed.Decode(); !errors.Is(err, eventrecord.ErrCorrupt) {
						t.Fatalf("decode error = %v, want eventrecord.ErrCorrupt", err)
					}
				})
			}
		})
	}
}

func TestEventRecordCanonicalReadbackRejectsDurableScalarRepairParity(t *testing.T) {
	mutations := []struct {
		name       string
		sqlite     string
		postgres   string
		value      string
		wantDetail string
	}{
		{name: "flow_instance", sqlite: `UPDATE events SET flow_instance = ? WHERE event_id = ?`, postgres: `UPDATE events SET flow_instance = $1 WHERE event_id = $2::uuid`, value: "/flow-a/one/", wantDetail: "flow_instance"},
	}
	for _, backend := range eventRecordContractBackends() {
		for _, mutation := range mutations {
			backend, mutation := backend, mutation
			t.Run(backend.name+"/"+mutation.name, func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				event := eventtest.RootIngress(
					uuid.NewString(), "readback.canonical", "gateway", "task", []byte(`{"ok":true}`), 0,
					uuid.NewString(), "", events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()), "flow-a/one"),
					time.Date(2026, 7, 18, 18, 45, 0, 123456000, time.UTC),
				)
				if err := commitSemanticEventFixture(ctx, fixture.store.(semanticEventFixtureStore), event); err != nil {
					t.Fatalf("commit event: %v", err)
				}
				eventtestsql.CorruptEventStore(t, ctx, fixture.db, fixture.dialect, eventtestsql.EventCorruptionClaim{
					Invariant: "store.event_record.canonical_readback",
					Reason:    "prove canonical readback rejects durable " + mutation.name + " repair",
				}, mutation.sqlite, mutation.postgres, mutation.value, event.ID())
				_, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
				if found || !errors.Is(err, eventrecord.ErrCorrupt) || !strings.Contains(err.Error(), mutation.wantDetail) {
					t.Fatalf("canonical adapter readback = found:%v err:%v, want typed %s corruption", found, err, mutation.wantDetail)
				}
			})
		}
	}
}

type countedEventRecordQueryer struct {
	db      *sql.DB
	queries int
}

func (q *countedEventRecordQueryer) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return q.db.QueryRowContext(ctx, query, args...)
}

func (q *countedEventRecordQueryer) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.queries++
	return q.db.QueryContext(ctx, query, args...)
}

type eventRecordContractBackend struct {
	name string
	open func(*testing.T) authorActivityReceiptFixture
}

func eventRecordContractBackends() []eventRecordContractBackend {
	return []eventRecordContractBackend{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	}
}

func assertExactEventRecord(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, event events.Event) {
	t.Helper()
	wantAdmitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit expected event: %v", err)
	}
	want, err := eventrecord.FromAdmitted(wantAdmitted)
	if err != nil {
		t.Fatalf("build expected record: %v", err)
	}
	got, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
	if err != nil || !found {
		t.Fatalf("load event record: found=%v err=%v", found, err)
	}
	if !want.Equal(got) {
		t.Fatalf("durable record differs:\nwant=%#v\n got=%#v", want, got)
	}
	decoded, err := decodeEventRecord(got)
	if err != nil {
		t.Fatalf("decode event record: %v", err)
	}
	decodedEvent := decoded.Event()
	if decoded.ID() != event.ID() || decodedEvent.AdmissionClass() != event.AdmissionClass() || !decodedEvent.Producer().Equal(event.Producer()) {
		t.Fatalf("decoded identity = %s/%s/%v", decoded.ID(), decodedEvent.AdmissionClass(), decodedEvent.Producer())
	}
}

var _ eventRecordContractStore = (*PostgresStore)(nil)
var _ eventRecordContractStore = (*SQLiteRuntimeStore)(nil)
