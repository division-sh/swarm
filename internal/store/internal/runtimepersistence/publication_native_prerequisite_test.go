package runtimepersistence

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	apiidempotencycontract "github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestPublicationFaultPrerequisitesConsumeOneNativeConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []PublicationFailureCut{PublicationAfterMaterialization, PublicationBeforeReplayScope, PublicationBeforeAPICompletion} {
			for _, scenario := range []string{"fields", "fieldless", "foreign_event", "foreign_run", "crossed_instance_readiness", "missing_declared_fields"} {
				t.Run(fmt.Sprintf("%s/%d/%s", backend, cut, scenario), func(t *testing.T) {
					proveNativePublicationPrerequisite(t, backend, cut, scenario)
				})
			}
		}
	}
}

func nativePublicationPrerequisiteFixture(t *testing.T, backend string, fields bool) (receiverConfigActivationFixture, pipeline.FlowInstanceActivationRequest, pipeline.FlowInstanceActivationPlan) {
	t.Helper()
	files := map[string]string{
		"schema.yaml":        "name: publication-prerequisites\n",
		"review/schema.yaml": "name: review\ninstance: request_id\nstages:\n  pending: {}\npins:\n  inputs: [construct.requested]\n",
		"review/events.yaml": "construct.requested:\n  request_id: text\n",
	}
	if fields {
		files["review/entities.yaml"] = "review_item:\n  request_id: text\n"
	} else {
		files = map[string]string{
			"schema.yaml": "name: publication-prerequisites\nstages:\n  pending: {}\npins:\n  inputs: [construct.requested]\n",
			"events.yaml": "construct.requested:\n",
		}
	}
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
	if !fields {
		run := correlation.RunIDFromContext(f.ctx)
		request := sqliteFlowActivationRequest(f.bundle, ".", run, "", run)
		request.Instance = flowidentity.Stored(request.ContractBundle, ".", run, run, run, "")
		input := semanticview.Wrap(f.bundle).FlowInputEventPins(".")[0].EventType()
		request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(input), "fixture", "", []byte(`{}`), 0, run, events.EventEnvelope{}, request.OccurredAt)
		plan := commitNativePublicationReceiver(t, f, request)
		return f, request, plan
	}
	f.constructKeylessRoot(t)
	request := f.request("one", "one", "")
	plan := commitNativePublicationReceiver(t, f, request)
	commitNativePublicationReceiver(t, f, f.request("sibling", "sibling", ""))
	return f, request, plan
}

func commitNativePublicationReceiver(t *testing.T, f receiverConfigActivationFixture, request pipeline.FlowInstanceActivationRequest) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct exact publication prerequisite: %+v %v", committed, err)
	}
	return plan
}

func nativePublicationPrerequisiteEvent(t *testing.T, f receiverConfigActivationFixture, request pipeline.FlowInstanceActivationRequest, plan pipeline.FlowInstanceActivationPlan, scenario string) events.Event {
	t.Helper()
	event := request.TriggerEvent
	switch scenario {
	case "foreign_event":
		event = eventtest.ExistingRunRootIngress(uuid.NewString(), event.Type(), "fixture", "", event.Payload(), 0, event.RunID(), events.EventEnvelope{}, event.CreatedAt())
	case "foreign_run":
		event = eventtest.RunCreatingRootIngress(uuid.NewString(), event.Type(), "fixture", "", event.Payload(), 0, uuid.NewString(), "", events.EventEnvelope{}, event.CreatedAt())
	case "crossed_instance_readiness":
		changed, err := RemoveDescriptorReadinessForTest(f.ctx, f.store, event.RunID(), plan.Instance.StorageRef)
		if err != nil || changed != 1 {
			t.Fatalf("remove only the exact receiver readiness: %d %v", changed, err)
		}
	case "missing_declared_fields":
		changed, err := RemoveWorkflowProjectionFieldsForTest(f.ctx, f.store, event.RunID(), plan.Instance.EntityID)
		if err != nil || changed != 1 {
			t.Fatalf("remove only the declared field row: %d %v", changed, err)
		}
	}
	return event
}

func proveNativePublicationPrerequisite(t *testing.T, backend string, cut PublicationFailureCut, scenario string) {
	t.Helper()
	f, request, plan := nativePublicationPrerequisiteFixture(t, backend, scenario != "fieldless")
	event := nativePublicationPrerequisiteEvent(t, f, request, plan, scenario)
	if cut == PublicationBeforeAPICompletion {
		if err := commitSemanticEventFixtureWithAgents(f.ctx, f.store, event, []string{"prerequisite-agent"}); err != nil {
			t.Fatal(err)
		}
	}
	fault := ownPublicationFailureControl(t, f.store, cut, plan.Instance.WorkflowName)
	// The later cleanup runs first, before the prearmed DDL restoration.
	t.Cleanup(func() {
		if err := f.manager.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if err := fault.InstallForTest(f.ctx); err != nil {
		t.Fatal(err)
	}
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	err = attemptNativePublicationFault(f.ctx, f.store, cut, event)
	marker := nativePublicationFaultMarker(cut, scenario == "fields" || scenario == "fieldless")
	if err == nil || !strings.Contains(err.Error(), marker) {
		t.Fatalf("wrong exact native prerequisite cut: %v, want %q", err, marker)
	}
	after, readErr := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if readErr != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("publication fault changed complete application evidence: %v", readErr)
	}
}

func attemptNativePublicationFault(ctx context.Context, selected agentFixtureFlowStore, cut PublicationFailureCut, event events.Event) error {
	if cut != PublicationBeforeAPICompletion {
		return commitSemanticEventFixtureWithAgents(ctx, selected, event, []string{"prerequisite-agent"})
	}
	owner := selected.(interface {
		WithAPIIdempotency(context.Context, apiidempotencycontract.Request, func(context.Context) (apiidempotencycontract.Completion, error)) (apiidempotencycontract.Completion, bool, error)
	})
	req := apiidempotencycontract.Request{Method: "event.publish", Actor: apiidempotencycontract.BearerActor("native-prerequisite"), IdempotencyKey: uuid.NewString(), RequestHash: "exact-native-prerequisite", ResourceID: event.ID(), Now: time.Now().UTC(), TTL: time.Hour}
	_, _, err := owner.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotencycontract.Completion, error) {
		return apiidempotencycontract.Completion{ResourceID: event.ID(), Response: []byte(`{}`)}, nil
	})
	return err
}

func nativePublicationFaultMarker(cut PublicationFailureCut, complete bool) string {
	if complete {
		switch cut {
		case PublicationAfterMaterialization:
			return "injected delivery route persistence failure"
		case PublicationBeforeReplayScope:
			return "injected committed replay-scope persistence failure"
		default:
			return "injected API idempotency completion persistence failure"
		}
	}
	switch cut {
	case PublicationAfterMaterialization:
		return "materialization fault prerequisite: exact lifecycle absent"
	case PublicationBeforeReplayScope:
		return "replay fault prerequisite: exact lifecycle absent"
	default:
		return "API completion fault prerequisite: exact lifecycle absent"
	}
}
