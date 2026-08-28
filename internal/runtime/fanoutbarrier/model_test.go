package fanoutbarrier

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestFoldTerminalDispositionAlgebra(t *testing.T) {
	tests := []struct {
		name     string
		fold     Fold
		valid    bool
		terminal bool
	}{
		{name: "zero", fold: Fold{Summary: Summary{}, EnumerationClosed: true}, valid: true, terminal: true},
		{name: "mixed terminal", fold: Fold{Summary: Summary{Total: 5, Succeeded: 1, DeadLettered: 1, NoRoute: 1, SemanticRejected: 1, Canceled: 1}, EnumerationClosed: true}, valid: true, terminal: true},
		{name: "open fully classified", fold: Fold{Summary: Summary{Total: 1, Succeeded: 1}}, valid: true},
		{name: "closed pending delivery", fold: Fold{Summary: Summary{Total: 2, Succeeded: 1}, EnumerationClosed: true, PendingCommitted: 1}, valid: true},
		{name: "closed missing ordinal", fold: Fold{Summary: Summary{Total: 2, Succeeded: 1}, EnumerationClosed: true}, valid: false},
		{name: "over classified", fold: Fold{Summary: Summary{Total: 1, Succeeded: 1}, PendingCommitted: 1}, valid: false},
		{name: "negative", fold: Fold{Summary: Summary{Total: 1}, PendingCommitted: -1}, valid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fold.Validate()
			if (err == nil) != tc.valid {
				t.Fatalf("Validate() error = %v, want valid %v", err, tc.valid)
			}
			if got := tc.fold.Terminal(); got != tc.terminal {
				t.Fatalf("Terminal() = %v, want %v", got, tc.terminal)
			}
		})
	}
}

func TestSummaryContextIsClosedAndTyped(t *testing.T) {
	want := Summary{Total: 10, Succeeded: 4, DeadLettered: 2, NoRoute: 1, SemanticRejected: 2, Canceled: 1}
	got, err := SummaryFromContext(want.Context())
	if err != nil || got != want {
		t.Fatalf("round trip summary = %#v err=%v", got, err)
	}
	for name, hostile := range map[string]map[string]any{
		"missing disposition": {"total": 1, "dispositions": map[string]any{"succeeded": 1, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0}},
		"extra root":          {"total": 1, "dispositions": map[string]any{"succeeded": 1, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0}, "routes": []any{}},
		"fractional":          {"total": 1.5, "dispositions": map[string]any{"succeeded": 1, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0}},
		"wrong sum":           {"total": 2, "dispositions": map[string]any{"succeeded": 1, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0}},
	} {
		t.Run(name, func(t *testing.T) {
			if value, err := SummaryFromContext(hostile); err == nil {
				t.Fatalf("hostile context decoded as %#v", value)
			}
		})
	}
}

func TestBarrierStateMachineRequiresExactEvidence(t *testing.T) {
	registration := testRegistration(t)
	summary := Summary{Total: 1, Succeeded: 1}
	now := registration.CreatedAt.Add(time.Second)
	tests := []struct {
		name  string
		value Barrier
		valid bool
	}{
		{name: "armed", value: Barrier{Registration: registration, Status: StatusArmed, UpdatedAt: now}, valid: true},
		{name: "armed with summary", value: Barrier{Registration: registration, Status: StatusArmed, Summary: &summary, UpdatedAt: now}},
		{name: "closed missing schedule", value: Barrier{Registration: registration, Status: StatusClosedPending, Summary: &summary, UpdatedAt: now}},
		{name: "closed", value: Barrier{Registration: registration, Status: StatusClosedPending, Summary: &summary, ScheduleKey: registration.Handle.TaskID(), UpdatedAt: now}, valid: true},
		{name: "fired", value: Barrier{Registration: registration, Status: StatusFired, Summary: &summary, ScheduleKey: registration.Handle.TaskID(), UpdatedAt: now}, valid: true},
		{name: "run terminal suppression", value: Barrier{Registration: registration, Status: StatusSuppressedRunTerminal, Summary: &summary, UpdatedAt: now}, valid: true},
		{name: "generation suppression before fold", value: Barrier{Registration: registration, Status: StatusSuppressedGenerationSuperseded, UpdatedAt: now}, valid: true},
		{name: "unknown status", value: Barrier{Registration: registration, Status: "unknown", Summary: &summary, UpdatedAt: now}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.value.Validate()
			if (err == nil) != tc.valid {
				t.Fatalf("Validate() error = %v, want valid %v", err, tc.valid)
			}
		})
	}
}

func TestRegistrationRejectsCrossIntentAndRoutingContradictions(t *testing.T) {
	registration := testRegistration(t)
	for name, mutate := range map[string]func(*Registration){
		"triggering delivery": func(value *Registration) { value.IntentKey.TriggeringDeliveryID = uuid.NewString() },
		"element":             func(value *Registration) { value.IntentKey.ElementRef.ElementID = uuid.NewString() },
		"route":               func(value *Registration) { value.Route.InstancePath = "worker/other" },
		"entity":              func(value *Registration) { value.EntityID = uuid.NewString() },
		"routing source": func(value *Registration) {
			value.RoutingSource, _ = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "other", FlowInstance: "other/inst", EntityID: value.EntityID})
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := registration
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("contradictory registration passed: %#v", candidate)
			}
		})
	}
}

func testRegistration(t *testing.T) Registration {
	t.Helper()
	triggeringDeliveryID := uuid.NewString()
	elementID := uuid.NewString()
	ref, err := timeridentity.NewFanOutDeliveryJoinRef(
		identitytest.FlowNode(t, "worker", "producer"),
		"batch.requested",
		"all-items-delivered",
		"root",
		elementID,
		"bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindFanOutIntent(triggeringDeliveryID, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	entityID := uuid.NewString()
	route := flowidentity.StoredRoute("worker", "inst", "worker/inst")
	routingSource, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/inst", EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	return Registration{
		IntentKey: fanoutobligation.IntentKey{
			RunID: uuid.NewString(), TriggeringDeliveryID: triggeringDeliveryID,
			ElementRef: runtimecontracts.FanOutElementRef{PackageKey: "root", ElementID: elementID},
		},
		Handle: handle, Route: route, EntityID: entityID, RoutingSource: routingSource,
		ExecutionMode: executionmode.Live, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
}
