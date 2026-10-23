package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestSystemNodeMutationRejectsPredecessorAfterProcessCutoverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []struct {
			name string
			mode mutationprotocol.Evidence
		}{{"engine", 0}, {"story", mutationprotocol.Story}, {"authority_fence", mutationprotocol.AuthorityFence}, {"revision_only", mutationprotocol.RevisionOnly}} {
			entrances := []string{"pooled"}
			if backend == "postgres" && operation.mode != 0 {
				entrances = append(entrances, "retained")
			}
			for _, entrance := range entrances {
				for _, cut := range []string{"current", "successor", "retired_grant", "missing", "foreign_store"} {
					t.Run(backend+"/"+operation.name+"/"+entrance+"/"+cut, func(t *testing.T) {
						proveSystemNodeProcessCutover(t, backend, operation.mode, entrance, cut)
					})
				}
			}
		}
	}
}

func proveSystemNodeProcessCutover(t *testing.T, backend string, mode mutationprotocol.Evidence, entrance, cut string) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	flowID := "system-node-process-fence"
	f, record := constructWorkflowMutationFixture(t, backend, flowID, at)
	selected := f.store.(stateOnlyAcquisitionStore)
	node := mustPersistenceNode(flowID, "predecessor-handler")
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{
			FlowID: flowID, FlowInstance: record.Identity.Route.InstancePath, EntityID: record.EntityID,
		}),
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "handler.requested", "fixture", "", []byte(`{}`), 0,
		correlation.RunIDFromContext(f.ctx),
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, record.EntityID), record.Identity.Route.InstancePath), at)
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(f.ctx, selected, event, route)
	if err != nil {
		t.Fatalf("claim predecessor system-node work: %v", err)
	}
	record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
	command := pipeline.WorkflowEngineMutationCommand{State: record, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{
		Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Second,
		RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
	}}
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	before, found, err := f.workflows.Load(f.ctx, record.Identity)
	if err != nil || !found {
		t.Fatalf("load predecessor state: found=%t err=%v", found, err)
	}
	process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	original, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := startupownership.FreezeMutationAuthority(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cut == "successor" || cut == "retired_grant" {
		if err := f.manager.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}
	switch cut {
	case "successor":
		if err := process.Release(f.ctx); err != nil {
			t.Fatal(err)
		}
		successor, err := f.store.(startupownership.Store).AcquireProcessCapability(context.Background(), testStartupAcquireRequest("system-node-successor"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = successor.Release(context.Background()) })
		next, err := successor.Evidence()
		if err != nil || next.AuthorityGeneration != original.AuthorityGeneration+1 {
			t.Fatalf("durable process cutover: before=%+v after=%+v err=%v", original, next, err)
		}
	case "retired_grant":
		if err := f.grant.Retire(f.ctx); err != nil {
			t.Fatal(err)
		}
	case "missing":
		ctx = context.Background()
	case "foreign_store":
		foreign, _ := constructWorkflowMutationFixture(t, backend, flowID, at)
		ctx, err = startupownership.WithRuntimeMutationAuthority(context.Background(), foreign.grant)
		if err != nil {
			t.Fatal(err)
		}
	}
	beforeFacts, err := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, event.RunID(), event.ID())
	if err != nil {
		t.Fatal(err)
	}
	activity := f.store.(authoractivity.Reader)
	beforeActivity, err := activity.HeadAuthorActivity(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var acknowledged, entered bool
	if mode == 0 {
		result, commitErr := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, command)
		acknowledged, err = result.Committed, commitErr
	} else {
		write := func(context.Context, *mutationprotocol.Attempt) (struct{}, error) {
			entered = true
			return struct{}{}, nil
		}
		var result mutationprotocol.Result[struct{}]
		switch store := f.store.(type) {
		case *PostgresStore:
			if entrance == "retained" {
				conn, connErr := f.db.Conn(context.Background())
				if connErr != nil {
					t.Fatal(connErr)
				}
				session, sessionErr := store.backend.NewSessionAuthority(conn)
				if sessionErr != nil {
					_ = conn.Close()
					t.Fatal(sessionErr)
				}
				defer session.Release()
				result = mutationprotocol.RunRetainedPostgres(ctx, session, mode, mutationprotocol.Ordinary, nil, nil, write)
			} else {
				result = mutationprotocol.RunPostgres(ctx, store.backend, mode, mutationprotocol.Ordinary, nil, nil, write)
			}
		case *SQLiteRuntimeStore:
			result = mutationprotocol.RunSQLite(ctx, store.backend, "system-node mutation admission", mode, mutationprotocol.Ordinary, nil, nil, write)
		default:
			t.Fatalf("unsupported selected fixture %T", f.store)
		}
		acknowledged, err = result.Acknowledged(), result.Err()
	}
	after, found, readErr := f.workflows.Load(f.ctx, record.Identity)
	afterFacts, factsErr := ReadSemanticEventFixtureEvidenceForTest(f.ctx, f.store, event.RunID(), event.ID())
	afterActivity, activityErr := activity.HeadAuthorActivity(f.ctx)
	if readErr != nil || !found || factsErr != nil || activityErr != nil {
		t.Fatalf("read post-command state: state=%v found=%t facts=%v activity=%v", readErr, found, factsErr, activityErr)
	}
	if cut == "current" {
		if err != nil || !acknowledged || mode != 0 && !entered || mode == 0 && reflect.DeepEqual(before, after) {
			t.Fatalf("current issuer must admit the valid command: acknowledged=%t entered=%t err=%v", acknowledged, entered, err)
		}
		return
	}
	if err == nil || acknowledged || entered || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(beforeFacts, afterFacts) || beforeActivity != afterActivity {
		t.Fatalf("noncurrent or absent original admitted a mutation: acknowledged=%t entered=%t err=%v", acknowledged, entered, err)
	}
}
