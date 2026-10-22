package runfork

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestRunStartPointRequiresExactRevisionWithoutEventEvidence(t *testing.T) {
	if RunForkPointRunStart != forkpoint.RunStart || string(RunForkPointRunStart) != "run_start" {
		t.Fatal("run start kind is not the canonical forkpoint alias")
	}
	point := RunForkPoint{Kind: RunForkPointRunStart, Revision: 7}
	if err := point.Validate(); err != nil {
		t.Fatal(err)
	}
	origin, err := point.MaterializationRunOrigin("source-run")
	if err != nil || origin.ForkPointKind() != runlifecycle.ForkOriginPointRunStart || origin.ForkRevision() != point.Revision || origin.SourceEventID() != "" {
		t.Fatalf("run-start origin lost typed coordinate: %+v, %v", origin, err)
	}
	raw, err := json.Marshal(point)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "event_id") {
		t.Fatalf("start point invented an event coordinate: %s", raw)
	}
	var decoded RunForkPoint
	if err := json.Unmarshal(raw, &decoded); err != nil || decoded != point {
		t.Fatalf("start point round trip: %+v, %v", decoded, err)
	}
	routingSource, err := events.NewExternalIngressRoutingSource("flow", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*RunForkPoint)
	}{
		{"zero_revision", func(p *RunForkPoint) { p.Revision = 0 }},
		{"negative_revision", func(p *RunForkPoint) { p.Revision = -1 }},
		{"event_id", func(p *RunForkPoint) { p.EventID = "11111111-1111-4111-8111-111111111111" }},
		{"input", func(p *RunForkPoint) { p.Input = "latest" }},
		{"event_name", func(p *RunForkPoint) { p.EventName = "created" }},
		{"source_event_id", func(p *RunForkPoint) { p.SourceEventID = "11111111-1111-4111-8111-111111111111" }},
		{"producer", func(p *RunForkPoint) { p.ProducedBy = "operator" }},
		{"producer_type", func(p *RunForkPoint) { p.ProducedByType = "platform" }},
		{"routing_source", func(p *RunForkPoint) { p.RoutingSource = routingSource }},
		{"timestamp", func(p *RunForkPoint) { p.Timestamp = time.Unix(1, 0).UTC() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broken := point
			tc.change(&broken)
			if err := broken.Validate(); err == nil {
				t.Fatalf("start point accepted event evidence: %+v", broken)
			}
		})
	}
}

func TestRunStartRequestsEncodeExplicitSelector(t *testing.T) {
	for _, request := range []any{RunForkPlanRequest{AtStart: true}, RunForkMaterializeRequest{AtStart: true}} {
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil || fields["at_start"] != true {
			t.Fatalf("request lost explicit at_start: %s, %v", raw, err)
		}
	}
	for _, request := range []any{RunForkPlanRequest{}, RunForkMaterializeRequest{}} {
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "at_start") {
			t.Fatalf("default request changed its JSON selector: %s", raw)
		}
	}
}
