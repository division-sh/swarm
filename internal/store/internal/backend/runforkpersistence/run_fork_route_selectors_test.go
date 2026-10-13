package runforkpersistence

import (
	"bytes"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestT17RouteSelectorsRetainCapturedEventFamilyWithoutSourceReads(t *testing.T) {
	snapshot, original, _ := replayCutFixture(t)
	selectors, err := historicalRouteSelectorPublications(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(selectors) != 1 {
		t.Fatalf("captured selectors=%d", len(selectors))
	}
	actual := selectors[0]
	left, _ := original.PayloadAdmission()
	right, admitted := actual.PayloadAdmission()
	if !admitted || !left.Binding().Equal(right.Binding()) || !bytes.Equal(actual.Payload(), original.Payload()) || actual.RoutingSource() != original.RoutingSource() || actual.RunID() != original.RunID() || actual.ID() != original.ID() {
		t.Fatalf("fixed selector changed publication/schema/source: %+v", actual)
	}
	plan := (runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: original.ID(), Revision: snapshot.Revision}}).WithHistoricalEvents(snapshot.Revision, []string{original.ID()})
	plan, err = plan.WithHistoricalRouteSelectors(snapshot.Revision, selectors)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Events[0].Payload = []byte(`{"later":"source mutation"}`)
	retained, found, err := plan.HistoricalRouteSelector(original.ID())
	if err != nil || !found || !bytes.Equal(retained.Payload(), original.Payload()) {
		t.Fatalf("retained selector followed mutable source: %v", err)
	}
	selectors[0] = events.Event{}
	if retained, found, err := plan.HistoricalRouteSelector(original.ID()); err != nil || !found || !bytes.Equal(retained.Payload(), original.Payload()) {
		t.Fatal("selector alias changed captured history")
	}
	for _, name := range []string{"foreign_run", "foreign_revision", "missing_membership"} {
		t.Run(name, func(t *testing.T) {
			changed := plan
			switch name {
			case "foreign_run":
				changed.SourceRunID = uuid.NewString()
			case "foreign_revision":
				changed.ForkPoint.Revision++
			case "missing_membership":
				changed = changed.WithHistoricalEvents(snapshot.Revision, nil)
			}
			if _, _, err := changed.HistoricalRouteSelector(original.ID()); err == nil {
				t.Fatal("foreign/missing selector accepted")
			}
		})
	}
}

func TestT17RouteSelectorsRefuseForeignDuplicateAndUnadmittedPublications(t *testing.T) {
	for _, name := range []string{"foreign_run", "duplicate", "future_revision", "missing_schema", "invalid_schema_class", "invalid_payload", "wrong_type", "wrong_producer"} {
		t.Run(name, func(t *testing.T) {
			snapshot, _, _ := replayCutFixture(t)
			switch name {
			case "foreign_run":
				snapshot.Events[0].RunID = uuid.NewString()
			case "duplicate":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "future_revision":
				snapshot.Events[0].Revision = snapshot.Revision + 1
			case "missing_schema":
				snapshot.Events[0].PayloadSchemaDigest = ""
			case "invalid_schema_class":
				snapshot.Events[0].PayloadSchemaClass = "invalid"
			case "invalid_payload":
				snapshot.Events[0].Payload = []byte(`{"invalid":`)
			case "wrong_type":
				snapshot.Events[0].EventName = string(events.EventTypePlatformRuntimeLog)
			case "wrong_producer":
				snapshot.Events[0].ProducedByType = string(events.EventProducerNode)
			}
			if _, err := historicalRouteSelectorPublications(snapshot); err == nil {
				t.Fatal("invalid captured selector accepted")
			}
		})
	}
}

func TestT17RouteSelectorsDoNotDecodeUnrelatedDiagnosticInternalOrMinimalHistory(t *testing.T) {
	for _, name := range []string{"runtime_control", "runtime_diagnostic", "diagnostic_direct", "platform_schema", "schema_less", "minimal_record", "absent_source", "platform_source"} {
		t.Run(name, func(t *testing.T) {
			snapshot, original, _ := replayCutFixture(t)
			unrelated := snapshot.Events[0]
			unrelated.EventID = uuid.NewString()
			// These facts do not supply authored-connect selectors. Their own
			// historical owners, not this optional projection, admit their bodies.
			unrelated.Payload = nil
			unrelated.PayloadSchemaDigest = ""
			switch name {
			case "runtime_control":
				unrelated.EventClass = string(events.EventAdmissionRuntimeControl)
			case "runtime_diagnostic":
				unrelated.EventClass = string(events.EventAdmissionRuntimeDiagnostic)
			case "diagnostic_direct":
				unrelated.EventClass = string(events.EventAdmissionDiagnosticDirect)
			case "platform_schema":
				unrelated.PayloadSchemaClass = events.PayloadSchemaPlatform
			case "schema_less":
				unrelated.PayloadSchemaClass = events.PayloadSchemaSchemaLess
			case "minimal_record":
				unrelated.EventClass = string(events.EventAdmissionUnknown)
				unrelated.PayloadSchemaClass = ""
			case "absent_source":
				unrelated.RoutingSource = events.NoRoutingSource()
			case "platform_source":
				unrelated.RoutingSource = events.NewPlatformControlRoutingSource()
			}
			snapshot.Events = append(snapshot.Events, unrelated)
			// Selector projection must not add a second delivery-family admission.
			delivery := snapshot.Deliveries[0]
			delivery.Snapshot.DeliveryID = uuid.NewString()
			delivery.Snapshot.EventID = unrelated.EventID
			snapshot.Deliveries = append(snapshot.Deliveries, delivery)
			before := projectionJSON(t, snapshot)
			selectors, err := historicalRouteSelectorPublications(snapshot)
			if err != nil || len(selectors) != 1 || selectors[0].ID() != original.ID() {
				t.Fatalf("unrelated %s became required selector evidence: count=%d err=%v", name, len(selectors), err)
			}
			if projectionJSON(t, snapshot) != before {
				t.Fatal("optional selector projection rewrote historical facts")
			}
			plan := (runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: original.ID(), Revision: snapshot.Revision}}).
				WithHistoricalEvents(snapshot.Revision, []string{original.ID(), unrelated.EventID})
			plan, err = plan.WithHistoricalRouteSelectors(snapshot.Revision, selectors)
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := plan.HistoricalRouteSelector(unrelated.EventID); err != nil || found {
				t.Fatalf("unrelated publication gained selector permission: found=%t error=%v", found, err)
			}
		})
	}
}
