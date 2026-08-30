package activityjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type routeSettlementDraftRecorder struct {
	drafts []runtimeauthoractivity.Draft
}

func (r *routeSettlementDraftRecorder) Record(_ context.Context, draft runtimeauthoractivity.Draft) error {
	r.drafts = append(r.drafts, draft)
	return nil
}

func (*routeSettlementDraftRecorder) PersistedOccurredAt(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (*routeSettlementDraftRecorder) PersistedAuthorSafeSummary(context.Context, string) (string, bool, error) {
	return "", false, nil
}

func TestNoDeliveryDispositionRendersAuthorWarningAndNDJSON(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	runID := uuid.NewString()
	parent := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.started", "gateway", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, now)
	instancePath := "review/instance-1"
	entityID := uuid.NewString()
	child := eventtest.ChildWithLineageAndRoutingSource(
		uuid.NewString(), events.EventType(instancePath+"/assessment.reported"), "reviewer", "", []byte(`{}`), 1,
		events.EventLineage{RunID: runID, ParentEventID: parent.ID(), ExecutionMode: executionmode.Live},
		events.EventEnvelope{FlowInstance: instancePath, EntityID: entityID, Scope: events.EventScopeEntity},
		eventtest.ConcreteTemplateRoutingSource("review", instancePath, entityID), now,
	)
	admitted, err := events.AdmitForPersistence(child, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	planID := events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("plan-a")))
	plan, err := events.NewConnectPlanEvaluation(planID, events.ConnectPlanNoRegistration, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger([]events.ConnectPlanEvaluation{plan})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryMatchedNoRecipient, ledger)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &routeSettlementDraftRecorder{}
	if err := RecordNoDeliveryWarning(context.Background(), recorder, admitted, settlement); err != nil {
		t.Fatal(err)
	}
	if len(recorder.drafts) != 1 {
		t.Fatalf("warning drafts = %d, want 1", len(recorder.drafts))
	}
	draft := recorder.drafts[0]
	if draft.Transition != "event_no_delivery" || draft.Projection.ReasonCode != "matched_no_recipient" || draft.Projection.InstancePath != instancePath || len(draft.Projection.PlanSHA256) != 1 || draft.Projection.PlanSHA256[0] != planID.String() {
		t.Fatalf("warning draft = %#v", draft)
	}
	draft.Scope = runtimeauthoractivity.BundleScope("runtime-1", "bundle-v2:sha256:"+strings.Repeat("1", 64))
	occurrence := runtimeauthoractivity.Occurrence{
		OccurrenceID: uuid.NewString(), Sequence: 1, Kind: draft.Kind, Version: runtimeauthoractivity.Version,
		Transition: draft.Transition, SourceOwner: draft.SourceOwner, SourceIdentity: draft.SourceIdentity,
		DedupKey: draft.DedupKey, OccurredAt: draft.OccurredAt, RunID: draft.RunID, EntityID: draft.EntityID,
		FlowID: draft.FlowID, Scope: draft.Scope, Projection: draft.Projection,
	}
	var rendered bytes.Buffer
	if err := runtimeauthoractivity.Render(&rendered, []runtimeauthoractivity.Occurrence{occurrence}, runtimeauthoractivity.RenderOptions{Mode: runtimeauthoractivity.RenderNDJSON}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"transition":"event_no_delivery"`, `"reason_code":"matched_no_recipient"`, `"instance_path":"review/instance-1"`, planID.String()} {
		if !strings.Contains(rendered.String(), want) {
			t.Fatalf("NDJSON = %s, want %s", rendered.String(), want)
		}
	}

	deliberate, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryNoSubscriberByDesign, ledger)
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordNoDeliveryWarning(context.Background(), recorder, admitted, deliberate); err != nil {
		t.Fatal(err)
	}
	if len(recorder.drafts) != 1 {
		t.Fatalf("deliberate no-subscriber emitted a warning")
	}
}
