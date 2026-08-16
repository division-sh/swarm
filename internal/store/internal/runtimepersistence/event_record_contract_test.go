package runtimepersistence

import (
	"bytes"
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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	"github.com/google/uuid"
)

type eventRecordContractStore interface {
	semanticEventFixtureStore
	diagnosticRuntimeLogFixtureStore
	CommitSelectedForkEvent(context.Context, runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error)
}

type eventDeliveryRouteReadbackStore interface {
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
}

type preparedPublishEventReadbackStore interface {
	LoadPreparedPublishEvent(context.Context, string) (runtimebus.PreparedPublishEvent, bool, error)
}

func TestEventRecordExactPersistenceParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(eventRecordContractStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 18, 16, 0, 0, 123456000, time.UTC)
			runID := uuid.NewString()
			root := eventtest.RunCreatingRootIngress(uuid.NewString(), "contract.root", "gateway", "root-task", []byte(`{"root":true}`), 1, runID, "", events.EventEnvelope{}, now)
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
			forkRoot := eventtest.RunCreatingRootIngress(
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
			owner := pipelineObligationOwnerForFixture(store)
			claim, err := owner.ClaimPublication(ctx, selected.ID())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = owner.Release(context.WithoutCancel(ctx), claim) }()
			outcome, err := commitSelectedForkEventOutcome(ctx, store, runtimebus.CommitSelectedForkEventRequest{
				Commit: runtimebus.CommitPublishRequest{
					Event: admitted, RouteSettlement: testRouteSettlement(admitted.Event(), nil), ReplayScope: runtimepipelineobligation.ScopeDirect, PipelineClaim: claim,
				},
				Lineage: runfork.RunForkSelectedContractExecutionLineage{
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

func TestPreparedPublishOutboxReadbackPreservesExactPayloadBytesParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(eventRecordContractStore)
			ctx := testAuthorActivityContext()
			payload := []byte("{\n  \"numeric\": 1.0, \"first\": true\n}")
			event := eventtest.RunCreatingRootIngress(
				uuid.NewString(), "outbox.payload_bytes", "gateway", "", payload, 0,
				uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
			)
			if err := commitSemanticEventFixture(ctx, store, event); err != nil {
				t.Fatalf("commit event: %v", err)
			}

			prepared, found, err := fixture.store.(preparedPublishEventReadbackStore).LoadPreparedPublishEvent(ctx, event.ID())
			if err != nil || !found {
				t.Fatalf("LoadPreparedPublishEvent = found:%v err:%v", found, err)
			}
			if got := prepared.Event.Event().Payload(); !bytes.Equal(got, payload) {
				t.Fatalf("prepared payload = %q, want %q", got, payload)
			}
		})
	}
}

func TestEventRoutingSourceRoundTripParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(eventRecordContractStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 18, 16, 30, 0, 0, time.UTC)
			for _, source := range routingSourceRoundTripFixtures(t) {
				t.Run(source.Kind().StorageCode(), func(t *testing.T) {
					event := eventtest.RunCreatingRootIngressWithRoutingSource(
						uuid.NewString(), events.EventType("routing.source."+source.Kind().StorageCode()), "gateway", "", []byte(`{}`), 0,
						uuid.NewString(), "", events.EventEnvelope{}, source, now,
					)
					if err := commitSemanticEventFixture(ctx, store, event); err != nil {
						t.Fatalf("commit event: %v", err)
					}
					record, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
					if err != nil || !found {
						t.Fatalf("load event record: found=%v err=%v", found, err)
					}
					admitted, err := record.Decode()
					if err != nil {
						t.Fatalf("decode event record: %v", err)
					}
					got := admitted.Event().RoutingSource()
					if got.Kind() != source.Kind() || got.Route() != source.Route() || got.Authority() != source.Authority() {
						t.Fatalf("routing source = %#v/%#v/%#v, want %#v/%#v/%#v",
							got.Kind(), got.Route(), got.Authority(), source.Kind(), source.Route(), source.Authority())
					}
				})
			}
		})
	}
}

func TestConnectExecutionClaimRoundTripAndGraphMutationParity(t *testing.T) {
	original := compiledConnectClaimFixture(t, canonicalrouting.TemplateInstanceRouteSelect, canonicalrouting.TemplateInstanceNoSecondPin, "deploy_completed")
	changed := compiledConnectClaimFixture(t, canonicalrouting.TemplateInstanceRouteSelect, canonicalrouting.TemplateInstanceSecondPinDistinctEvent, "deploy_audited")
	if original.ConnectClaim.Equal(changed.ConnectClaim) {
		t.Fatal("fixture mutation did not change the compiled connect claim")
	}

	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(
				uuid.NewString(), "routing.claim.roundtrip", "gateway", "", []byte(`{}`), 0,
				uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 18, 16, 45, 0, 0, time.UTC),
			)
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{original}); err != nil {
				t.Fatalf("commit event and route: %v", err)
			}
			routes, err := fixture.store.(eventDeliveryRouteReadbackStore).ListEventDeliveryRoutes(ctx, event.ID())
			if err != nil {
				t.Fatalf("list event delivery routes: %v", err)
			}
			if len(routes) != 1 || !routes[0].ConnectClaim.Equal(original.ConnectClaim) {
				t.Fatalf("restored routes = %#v, want original stamped claim", routes)
			}
			if routes[0].ConnectClaim.Equal(changed.ConnectClaim) {
				t.Fatal("restored claim was rematched against the changed graph")
			}
			want, err := original.ConnectClaim.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			got, err := routes[0].ConnectClaim.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("restored claim bytes = %s, want %s", got, want)
			}
		})
	}
}

func routingSourceRoundTripFixtures(t testing.TB) []events.RoutingSource {
	t.Helper()
	must := func(source events.RoutingSource, err error) events.RoutingSource {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return source
	}
	return []events.RoutingSource{
		events.NoRoutingSource(),
		must(events.NewExternalIngressRoutingSource("gateway", uuid.NewString(), events.RoutingSourceAuthorityProviderAdmissionPlan)),
		must(events.NewRootRoutingSource(uuid.NewString())),
		must(events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "static", FlowInstance: "static", EntityID: uuid.NewString()})),
		must(events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: "template", FlowInstance: "template/one", EntityID: uuid.NewString()})),
		must(events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "control", FlowInstance: "control/one", EntityID: uuid.NewString()})),
		events.NewPlatformControlRoutingSource(),
	}
}

func compiledConnectClaimFixture(t testing.TB, mode canonicalrouting.TemplateInstanceRouteMode, secondPin canonicalrouting.TemplateInstanceSecondPin, receiverPin string) events.DeliveryRoute {
	t.Helper()
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{
		Mode: mode, SecondPin: secondPin,
	})
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load canonical connect fixture: %v", err)
	}
	var selected runtimepinrouting.ConnectRoutePlan
	for _, plan := range runtimepinrouting.CompileConnectGraph(semanticview.Wrap(bundle)).Plans() {
		if plan.ReceiverEndpoint().Readback().Pin == receiverPin {
			selected = plan
			break
		}
	}
	if selected.ReceiverEndpoint().Readback().Pin == "" {
		t.Fatalf("compiled connect graph has no receiver pin %q", receiverPin)
	}
	target := events.RouteIdentity{FlowID: "claim-flow", FlowInstance: "claim-flow"}
	blueprint := runtimepinrouting.ConnectDeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustPersistenceNode("claim-flow", "claim-node")), Target: target,
		Handler: runtimepinrouting.MustConnectReceiverHandler(mustPersistenceNode("claim-flow", "claim-node")),
	}
	claim, err := runtimepinrouting.ConnectExecutionClaim(selected, blueprint)
	if err != nil {
		t.Fatalf("mint connect execution claim: %v", err)
	}
	route := events.DeliveryRoute{
		Recipient:    blueprint.Recipient,
		Target:       events.MustEntitylessReceiverTarget(target),
		ConnectClaim: claim,
	}
	return route
}

func TestEventRecordEveryFieldDuplicateParity(t *testing.T) {
	baseEvent := eventtest.RunCreatingRootIngress(uuid.NewString(), "duplicate.base", "gateway", "task", []byte(`{"value":1}`), 2, uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 18, 17, 0, 0, 0, time.UTC))
	admitted, err := events.AdmitForPersistence(baseEvent, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	base, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
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
		{"routing_source_kind", func(r *persistedEventIdentity) { r.RoutingSourceKind = "concrete_template_instance" }},
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
			if duplicate, err := resolveExistingEventIdentity(base.EventID, base, changed, true); duplicate || !errors.Is(err, events.ErrEventIdentityConflict) {
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
			conflict := eventtest.RunCreatingRootIngress(baseEvent.ID(), baseEvent.Type(), baseEvent.SourceAgent(), baseEvent.TaskID(), []byte(`{"value":2}`), baseEvent.ChainDepth(), baseEvent.RunID(), "", baseEvent.NormalizedEnvelope(), baseEvent.CreatedAt())
			if _, err := commitSemanticEventFixtureOutcome(ctx, store, conflict, nil, "direct"); !errors.Is(err, events.ErrEventIdentityConflict) {
				t.Fatalf("conflicting duplicate error = %v", err)
			}
			nestedID := uuid.NewString()
			nested := eventtest.RunCreatingRootIngress(nestedID, "duplicate.nested", "gateway", "task", []byte(`{"nested":{"a":null}}`), 0, uuid.NewString(), "", events.EventEnvelope{}, baseEvent.CreatedAt())
			if outcome, err := commitSemanticEventFixtureOutcome(ctx, store, nested, nil, "direct"); err != nil || outcome != runtimebus.EventAppendInserted {
				t.Fatalf("nested initial commit: outcome=%v err=%v", outcome, err)
			}
			nestedConflict := eventtest.RunCreatingRootIngress(nestedID, nested.Type(), nested.SourceAgent(), nested.TaskID(), []byte(`{"nested":{"b":null}}`), 0, nested.RunID(), "", events.EventEnvelope{}, nested.CreatedAt())
			if _, err := commitSemanticEventFixtureOutcome(ctx, store, nestedConflict, nil, "direct"); !errors.Is(err, events.ErrEventIdentityConflict) {
				t.Fatalf("nested null-key duplicate error = %v", err)
			}
		})
	}
}

func TestEventRecordPayloadByteIdentityParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range []struct {
				name               string
				original, conflict []byte
			}{
				{name: "whitespace", original: []byte(`{"value":1}`), conflict: []byte(`{ "value": 1 }`)},
				{name: "key_order", original: []byte(`{"a":1,"b":2}`), conflict: []byte(`{"b":2,"a":1}`)},
				{name: "numeric_lexeme", original: []byte(`{"value":1}`), conflict: []byte(`{"value":1.0}`)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fixture := backend.open(t)
					store := fixture.store.(semanticEventFixtureStore)
					ctx := testAuthorActivityContext()
					now := time.Date(2026, 8, 13, 18, 0, 0, 0, time.UTC)
					event := eventtest.RunCreatingRootIngress(
						uuid.NewString(), "payload.bytes", "gateway", "", tc.original, 0,
						uuid.NewString(), "", events.EventEnvelope{}, now,
					)
					outcome, err := commitSemanticEventFixtureOutcome(ctx, store, event, nil, "direct")
					if err != nil || outcome != runtimebus.EventAppendInserted {
						t.Fatalf("initial commit: outcome=%v err=%v", outcome, err)
					}
					record, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
					if err != nil || !found {
						t.Fatalf("load exact record: found=%v err=%v", found, err)
					}
					if !bytes.Equal(record.Payload, tc.original) {
						t.Fatalf("payload bytes = %q, want %q", record.Payload, tc.original)
					}
					outcome, err = commitSemanticEventFixtureOutcome(ctx, store, event, nil, "direct")
					if err != nil || outcome != runtimebus.EventAppendExactDuplicate {
						t.Fatalf("exact duplicate: outcome=%v err=%v", outcome, err)
					}
					conflict := eventtest.RunCreatingRootIngress(
						event.ID(), event.Type(), event.SourceAgent(), event.TaskID(), tc.conflict, event.ChainDepth(),
						event.RunID(), "", event.NormalizedEnvelope(), event.CreatedAt(),
					)
					if _, err := commitSemanticEventFixtureOutcome(ctx, store, conflict, nil, "direct"); !errors.Is(err, events.ErrEventIdentityConflict) {
						t.Fatalf("byte-distinct semantic duplicate error = %v, want identity conflict", err)
					}
				})
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
			payloads := make(map[string][]byte, len(ids))
			for index := range ids {
				ids[index] = uuid.NewString()
				eventAt := createdAt.Add(time.Duration(index) * time.Microsecond)
				payload := []byte(fmt.Sprintf("{\n  \"index\" : %d.0\n}", index))
				payloads[ids[index]] = payload
				var event events.Event
				if index == 0 {
					event = eventtest.RunCreatingRootIngress(
						ids[index], "batch.contract", "gateway", fmt.Sprintf("task-%d", index),
						payload, 0, runID, "", events.EventEnvelope{}, eventAt,
					)
				} else {
					event = eventtest.ExistingRunRootIngress(
						ids[index], "batch.contract", "gateway", fmt.Sprintf("task-%d", index),
						payload, 0, runID, events.EventEnvelope{}, eventAt,
					)
				}
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
						if !bytes.Equal(records[index].Payload, payloads[requested[index]]) {
							t.Fatalf("record %s payload = %q, want %q", requested[index], records[index].Payload, payloads[requested[index]])
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

func TestPostgresEventRecordReadbackNormalizesSessionTimezone(t *testing.T) {
	fixture := openPostgresAuthorActivityReceiptFixture(t)
	store := fixture.store.(semanticEventFixtureStore)
	ctx := testAuthorActivityContext()
	createdAt := time.Date(2026, 7, 18, 18, 45, 12, 123456000, time.UTC)
	event := eventtest.RunCreatingRootIngress(
		uuid.NewString(), "timezone.contract", "gateway", "timezone-task", []byte(`{"timezone":true}`),
		0, uuid.NewString(), "", events.EventEnvelope{}, createdAt,
	)
	if err := commitSemanticEventFixture(ctx, store, event); err != nil {
		t.Fatalf("commit event: %v", err)
	}

	conn, err := fixture.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.ExecContext(ctx, `SET TIME ZONE 'Pacific/Chatham'`); err != nil {
		t.Fatalf("set non-UTC session timezone: %v", err)
	}
	var rawCreatedAt time.Time
	if err := conn.QueryRowContext(ctx, `SELECT created_at FROM events WHERE event_id = $1::uuid`, event.ID()).Scan(&rawCreatedAt); err != nil {
		t.Fatalf("read raw timestamp: %v", err)
	}
	if _, offset := rawCreatedAt.Zone(); offset == 0 {
		t.Fatalf("raw timestamp offset = %d, want non-UTC proof precondition", offset)
	}

	record, found, err := eventrecordpostgres.Load(ctx, conn, event.ID())
	if err != nil || !found {
		t.Fatalf("load canonical record: found=%v err=%v", found, err)
	}
	assertCanonicalRecordTime(t, record.CreatedAt, createdAt)

	records, err := eventrecordpostgres.LoadMany(ctx, conn, []string{event.ID()})
	if err != nil {
		t.Fatalf("load canonical record batch: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	assertCanonicalRecordTime(t, records[0].CreatedAt, createdAt)
}

func assertCanonicalRecordTime(t *testing.T, got, want time.Time) {
	t.Helper()
	if !got.Equal(want) {
		t.Fatalf("created_at = %s, want %s", got, want)
	}
	if _, offset := got.Zone(); offset != 0 {
		t.Fatalf("created_at offset = %d, want UTC", offset)
	}
}

func TestEventRecordDecoderRejectsMalformedDurableFactsParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(
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
				{"runtime_source_without_route", func(record *persistedEventIdentity) { record.RoutingSourceKind = "concrete_template_instance" }},
				{"root_source_with_flow_identity", func(record *persistedEventIdentity) {
					record.RoutingSourceKind = events.RoutingSourceRoot.StorageCode()
					record.SourceRoute = []byte(fmt.Sprintf(`{"flow_id":"flow-a","flow_instance":"flow-a/one","entity_id":%q}`, uuid.NewString()))
				}},
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
				event := eventtest.RunCreatingRootIngress(
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
	want, err := eventrecord.FromAdmitted(wantAdmitted, testRouteSettlement(wantAdmitted.Event(), nil))
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
