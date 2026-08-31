package fanoutobligation

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

func TestMarshalCapsulePreservesEveryDynamicNumericCarrier(t *testing.T) {
	capsule := validIntentRequest(t).Capsule
	for _, target := range []*map[string]any{
		&capsule.Entity,
		&capsule.PlatformEntity,
		&capsule.Computed,
		&capsule.Accumulated,
		&capsule.Join,
		&capsule.Loop,
		&capsule.StateFields,
		&capsule.StateBookkeeping,
	} {
		*target = map[string]any{
			"integer": int64(75),
			"double":  float64(75),
			"nested":  []any{json.Number("75.0"), json.Number("75e0")},
		}
	}
	raw, err := MarshalCapsule(capsule)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"entity", "platform_entity", "computed", "accumulated", "join", "loop", "state_fields", "state_bookkeeping"} {
		if got := string(decoded[field]); got != `{"double":75.0,"integer":75,"nested":[75.0,75.0]}` {
			t.Fatalf("capsule %s = %s", field, got)
		}
	}
}

func TestIntentRequestRejectsContradictoryDurableIdentityAndCapsuleFacts(t *testing.T) {
	valid := validIntentRequest(t)
	for _, tc := range []struct {
		name   string
		mutate func(*IntentRequest)
	}{
		{name: "missing bundle", mutate: func(r *IntentRequest) { r.PlanRef.BundleHash = "" }},
		{name: "missing plan digest", mutate: func(r *IntentRequest) { r.PlanRef.SemanticDigest = "" }},
		{name: "different plan element", mutate: func(r *IntentRequest) { r.PlanRef.ElementRef.ElementID = uuid.NewString() }},
		{name: "different triggering event", mutate: func(r *IntentRequest) { r.Source.EventID = uuid.NewString() }},
		{name: "different entity source", mutate: func(r *IntentRequest) {
			r.Source = SourceRef{Kind: SourceEntityField, RunID: r.Key.RunID, EntityID: uuid.NewString(), Field: "items"}
			r.Capsule.EntityID = uuid.NewString()
		}},
		{name: "negative cardinality", mutate: func(r *IntentRequest) { r.Cardinality = -1 }},
		{name: "missing handler", mutate: func(r *IntentRequest) { r.Capsule.HandlerEventKey = "" }},
		{name: "missing route", mutate: func(r *IntentRequest) { r.Capsule.Route = runtimeflowidentity.Route{} }},
		{name: "missing producer source", mutate: func(r *IntentRequest) { r.Capsule.ProducerSource = events.RoutingSource{} }},
		{name: "invalid execution mode", mutate: func(r *IntentRequest) { r.Capsule.Lineage.ExecutionMode = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := valid
			tc.mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatalf("hostile request unexpectedly validated: %#v", request)
			}
		})
	}
}

func TestSourceRefClosedUnionRejectsCrossKindFacts(t *testing.T) {
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	for _, tc := range []struct {
		name      string
		source    SourceRef
		persisted bool
	}{
		{name: "payload with run", source: SourceRef{Kind: SourceEventPayloadField, EventID: eventID, RunID: runID, Field: "items"}},
		{name: "payload without event", source: SourceRef{Kind: SourceEventPayloadField, Field: "items"}},
		{name: "entity without run", source: SourceRef{Kind: SourceEntityField, EntityID: entityID, Field: "items"}},
		{name: "entity with event", source: SourceRef{Kind: SourceEntityField, RunID: runID, EntityID: entityID, EventID: eventID, Field: "items"}},
		{name: "persisted entity without revision", source: SourceRef{Kind: SourceEntityField, RunID: runID, EntityID: entityID, Field: "items"}, persisted: true},
		{name: "resource without declaration", source: SourceRef{Kind: SourceResourceVersion, VersionID: "resource-version-v1:sha256:0000000000000000000000000000000000000000000000000000000000000000"}},
		{name: "unknown kind", source: SourceRef{Kind: "current_entity_fallback", Field: "items"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.source.Validate(tc.persisted); err == nil {
				t.Fatalf("hostile source unexpectedly validated: %#v", tc.source)
			}
		})
	}
}

func validIntentRequest(t *testing.T) IntentRequest {
	t.Helper()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	element := runtimecontracts.FanOutElementRef{PackageKey: "root", ElementID: uuid.NewString()}
	producer, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return IntentRequest{
		Key: IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString(), ElementRef: element},
		PlanRef: runtimecontracts.FanOutPlanRef{
			BundleHash: "bundle-v1:sha256:0000000000000000000000000000000000000000000000000000000000000000",
			ElementRef: element, SemanticDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		},
		Source:      SourceRef{Kind: SourceEventPayloadField, EventID: eventID, Field: "items"},
		Cardinality: 3,
		Capsule: Capsule{
			NodeKey: "root.fan-out", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
			HandlerEventKey: "items.ready", ProducerSource: producer,
			Lineage:      events.EventLineage{RunID: runID, ParentEventID: eventID, ExecutionMode: "live"},
			CurrentState: "ready", ChainDepth: 0,
		},
	}
}

func TestIntentRejectsPersistedSourceOrProgressThatDisagreesWithRequest(t *testing.T) {
	request := validIntentRequest(t)
	now := time.Now().UTC()
	valid := Intent{Request: request, Source: request.Source, Status: StatusOpen, NextChunkSize: InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	for _, tc := range []struct {
		name   string
		mutate func(*Intent)
	}{
		{name: "different source field", mutate: func(i *Intent) { i.Source.Field = "other" }},
		{name: "cursor past cardinality", mutate: func(i *Intent) { i.Cursor = i.Request.Cardinality + 1 }},
		{name: "open at cardinality", mutate: func(i *Intent) { i.Cursor = i.Request.Cardinality }},
		{name: "oversized chunk", mutate: func(i *Intent) { i.NextChunkSize = MaxChunkSize + 1 }},
		{name: "lease without owner", mutate: func(i *Intent) { i.LeaseExpiresAt = now.Add(time.Minute) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intent := valid
			tc.mutate(&intent)
			if err := intent.Validate(); err == nil {
				t.Fatalf("hostile persisted intent unexpectedly validated: %#v", intent)
			}
		})
	}
}
