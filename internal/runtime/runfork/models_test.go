package runfork

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestRunForkContractFrontierRecipientUsesPrivateTypedWireCodec(t *testing.T) {
	want := frontierModelEvidence(t, frontierModelNodeInput(t), "edge", "pin")
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got RunForkContractFrontierRecipient
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	equal, err := forkrecipient.Equal(got, want)
	if err != nil || !equal || got.RouteSourceCode() != want.RouteSourceCode() {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}

	for _, hostile := range []string{
		`{"subscriber_type":"platform","subscriber_id":"worker"}`,
		`{"subscriber_type":"node","subscriber_id":"worker","unknown":true}`,
	} {
		if err := json.Unmarshal([]byte(hostile), &got); err == nil {
			t.Fatalf("Unmarshal(%s) succeeded, want fail-closed private codec", hostile)
		}
	}
	if _, err := json.Marshal(RunForkContractFrontierRecipient{}); err == nil {
		t.Fatalf("Marshal(zero) error = %v, want required recipient", err)
	}
	for _, mutate := range []func(map[string]any){
		func(w map[string]any) { w["unknown"] = true },
		func(w map[string]any) { delete(w, "handler_event") },
		func(w map[string]any) { delete(w, "authority") },
		func(w map[string]any) { w["authority"].(map[string]any)["unknown"] = true },
		func(w map[string]any) { w["authority"].(map[string]any)["kind"] = "future" },
		func(w map[string]any) { w["handler_node"] = identitytest.FlowNode(t, "review", "other") },
	} {
		var wire map[string]any
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		mutate(wire)
		hostile, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(hostile, &got); err == nil {
			t.Fatalf("Unmarshal(%s) accepted invalid evidence", hostile)
		}
	}
}

func TestRunForkContractFrontierEvidenceBindingAgentPlanAxes(t *testing.T) {
	plan := frontierModelAgentPlan(t)
	input := forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient(plan.AgentID()), Path: "review/inst-1", AgentPlan: plan, HandlerEvent: "work.received", RouteSource: "selected_source"}
	base := frontierModelEvidence(t, input, "", "")
	want := frontierModelFingerprint(t, RunForkContractFrontierEvent{DerivedRecipients: []RunForkContractFrontierRecipient{base}})
	tests := []struct {
		name   string
		change func(*agentidentity.Plan)
	}{
		{"name_agent_id", func(p *agentidentity.Plan) { p.Name.AgentID = "other-agent" }},
		{"name_owner", func(p *agentidentity.Plan) { p.Name.Owner = "other-owner" }},
		{"name_source", func(p *agentidentity.Plan) { p.Name.Source = agentidentity.NameSourceRuntimeCreated }},
		{"route_presence", func(p *agentidentity.Plan) { p.Route = agentidentity.RootRoute() }},
		{"route_scope", func(p *agentidentity.Plan) { p.Route.ScopeKey = "other-scope" }},
		{"route_instance", func(p *agentidentity.Plan) { p.Route.InstanceID = "other-instance" }},
		{"route_path", func(p *agentidentity.Plan) { p.Route.InstancePath = "other/path" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := input
			tc.change(&changed.AgentPlan)
			changed.Recipient = events.MustAgentDeliveryRecipient(changed.AgentPlan.AgentID())
			evidence := frontierModelEvidence(t, changed, "", "")
			if got := frontierModelFingerprint(t, RunForkContractFrontierEvent{DerivedRecipients: []RunForkContractFrontierRecipient{evidence}}); got == want {
				t.Fatal("changed canonical plan axis did not change frontier fingerprint")
			}
			if got := frontierModelFingerprint(t, RunForkContractFrontierEvent{DerivedRecipients: []RunForkContractFrontierRecipient{base, evidence}}); got == want {
				t.Fatal("distinct canonical plan was collapsed from frontier set")
			}
		})
	}
}

func TestRunForkContractFrontierEvidenceBindingSelectedAuthorityAxes(t *testing.T) {
	input := frontierModelNodeInput(t)
	base := frontierModelEvidence(t, input, "edge", "pin")
	want := frontierModelFingerprint(t, RunForkContractFrontierEvent{DerivedRecipients: []RunForkContractFrontierRecipient{base}})
	tests := []struct {
		name      string
		change    func(*forkrecipient.Input)
		edge, pin string
	}{
		{"handler_owner", func(in *forkrecipient.Input) {
			in.HandlerNode = identitytest.FlowNode(t, "review", "other")
			in.Recipient = events.MustNodeDeliveryRecipient(in.HandlerNode)
		}, "edge", "pin"},
		{"handler_event", func(in *forkrecipient.Input) { in.HandlerEvent = "other.received" }, "edge", "pin"},
		{"path", func(in *forkrecipient.Input) { in.Path = "review/inst-2" }, "edge", "pin"},
		{"local_vs_connect", func(*forkrecipient.Input) {}, "", ""},
		{"connect_plan", func(*forkrecipient.Input) {}, "other-edge", "pin"},
		{"receiver_pin", func(*forkrecipient.Input) {}, "edge", "other-pin"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := input
			tc.change(&changed)
			evidence := frontierModelEvidence(t, changed, tc.edge, tc.pin)
			if got := frontierModelFingerprint(t, RunForkContractFrontierEvent{DerivedRecipients: []RunForkContractFrontierRecipient{evidence}}); got == want {
				t.Fatal("changed selected authority did not change frontier fingerprint")
			}
		})
	}
}

func TestRunForkContractFrontierEvidenceBindingDiagnosticAndOrderingInvariance(t *testing.T) {
	input := frontierModelNodeInput(t)
	first := frontierModelEvidence(t, input, "edge", "pin")
	input.RouteSource = "different_diagnostic_only"
	alias := frontierModelEvidence(t, input, "edge", "pin")
	input.HandlerEvent = "other.received"
	second := frontierModelEvidence(t, input, "edge", "pin")
	historical := frontierModelHistoricalRoute(t)
	otherHistorical := historical
	otherHistorical.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "other-reply"}}
	event := RunForkContractFrontierEvent{
		SourceEventID: "source-event", EventName: "work.published", SourceClassifications: []string{"pending", "failed"},
		DerivedRecipients: []RunForkContractFrontierRecipient{first, second}, HistoricalDeliveryRoutes: []events.DeliveryRoute{historical, otherHistorical},
	}
	before, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	want := frontierModelFingerprint(t, event)
	after, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("binding mutated frontier input")
	}
	event.DerivedRecipients = []RunForkContractFrontierRecipient{second, alias, first}
	event.HistoricalDeliveryRoutes = []events.DeliveryRoute{otherHistorical, historical}
	event.SourceClassifications = []string{"failed", "pending", "pending"}
	if got := frontierModelFingerprint(t, event); got != want {
		t.Fatal("diagnostic, exact S duplicates or ordering changed semantic binding")
	}
	// Equal event coordinates must still sort by complete evidence, not input order.
	otherEvent := event
	otherEvent.DerivedRecipients = []RunForkContractFrontierRecipient{second}
	if frontierModelFingerprint(t, event, otherEvent) != frontierModelFingerprint(t, otherEvent, event) {
		t.Fatal("tied event coordinates make binding depend on input order")
	}
}

func TestRunForkContractFrontierEvidenceBindingPreservesHistoricalRoutes(t *testing.T) {
	historical := frontierModelHistoricalRoute(t)
	selected := frontierModelEvidence(t, frontierModelNodeInput(t), "selected-edge", "selected-pin")
	event := RunForkContractFrontierEvent{SourceEventID: "source-event", EventName: "work.published", DerivedRecipients: []RunForkContractFrontierRecipient{selected}, HistoricalDeliveryRoutes: []events.DeliveryRoute{historical}}
	want := frontierModelFingerprint(t, event)
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RunForkContractFrontierEvent
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.HistoricalDeliveryRoutes, event.HistoricalDeliveryRoutes) {
		t.Fatal("frontier wire lost full historical route evidence")
	}
	if got := frontierModelFingerprint(t, decoded); got != want {
		t.Fatal("roundtrip changed H/S binding")
	}
	tests := []struct {
		name   string
		change func(*events.DeliveryRoute)
	}{
		{"source_run", func(r *events.DeliveryRoute) { r.AgentIdentity.RunID = "22222222-2222-4222-8222-222222222222" }},
		{"source_agent_owner", func(r *events.DeliveryRoute) { r.AgentIdentity.Name.Owner = "other-owner" }},
		{"connect_absence", func(r *events.DeliveryRoute) { r.ConnectClaim = events.ConnectExecutionClaim{} }},
		{"connect_edge", func(r *events.DeliveryRoute) {
			r.ConnectClaim = frontierModelClaim(t, r.Recipient, "other-edge", "pin", "work.received")
		}},
		{"connect_pin", func(r *events.DeliveryRoute) {
			r.ConnectClaim = frontierModelClaim(t, r.Recipient, "edge", "other-pin", "work.received")
		}},
		{"handler_event", func(r *events.DeliveryRoute) {
			r.ConnectClaim = frontierModelClaim(t, r.Recipient, "edge", "pin", "other.received")
		}},
		{"target_kind", func(r *events.DeliveryRoute) { r.Target = events.MustMaterializingEntityTarget(r.Target.Route()) }},
		{"target_entity", func(r *events.DeliveryRoute) {
			target := r.Target.Route()
			target.EntityID = "other-entity"
			r.Target = events.MustExistingEntityTarget(target)
		}},
		{"reply", func(r *events.DeliveryRoute) {
			r.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "other-reply"}}
		}},
		{"projection", func(r *events.DeliveryRoute) {
			var err error
			r.PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"instance": "other"})
			if err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := historical
			tc.change(&changed)
			candidate := event
			candidate.HistoricalDeliveryRoutes = []events.DeliveryRoute{changed}
			if got := frontierModelFingerprint(t, candidate); got == want {
				t.Fatal("historical authority change did not change binding")
			}
			if !reflect.DeepEqual(candidate.DerivedRecipients, event.DerivedRecipients) {
				t.Fatal("H mutation changed S")
			}
		})
	}
	withoutHistory := event
	withoutHistory.HistoricalDeliveryRoutes = nil
	if frontierModelFingerprint(t, withoutHistory) == want {
		t.Fatal("binding omitted historical evidence")
	}
	withoutSelected := event
	withoutSelected.DerivedRecipients = nil
	if frontierModelFingerprint(t, withoutSelected) == want {
		t.Fatal("historical evidence substituted for selected authority")
	}
}

func TestRunForkSelectedContractRouteEventKeepsHistoricalAndSelectedEvidenceSeparate(t *testing.T) {
	historical := frontierModelHistoricalRoute(t)
	selected := frontierModelEvidence(t, frontierModelNodeInput(t), "selected-edge", "selected-pin")
	want := RunForkSelectedContractRouteEvent{
		SourceEventID: "source-event", EventName: "work.published",
		HistoricalDeliveryRoutes: []events.DeliveryRoute{historical},
		DerivedRecipients:        []RunForkContractFrontierRecipient{selected},
		Disposition:              RunForkSelectedContractDispositionEvidenceOnly,
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got RunForkSelectedContractRouteEvent
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.HistoricalDeliveryRoutes, want.HistoricalDeliveryRoutes) || len(got.DerivedRecipients) != 1 {
		t.Fatal("historical route event lost its distinct H/S evidence")
	}
	equal, err := forkrecipient.Equal(got.DerivedRecipients[0], selected)
	if err != nil || !equal {
		t.Fatalf("selected evidence changed: %v", err)
	}
	if got.DerivedRecipients[0].Recipient == historical.Recipient {
		t.Fatal("historical recipient replaced the independently selected recipient")
	}
}

func TestRunForkContractFrontierEvidenceBindingSourceEventCoordinates(t *testing.T) {
	frontier := RunForkContractFrontierAdmission{
		FrontierEventCount: 99,
		FrontierEvents: []RunForkContractFrontierEvent{
			{SourceEventID: "b", EventName: "work.received"},
			{SourceEventID: "a", EventName: "work.received"},
			{SourceEventID: "b", EventName: "other.received"},
		},
	}
	count, ids, fingerprint, err := RunForkContractFrontierEvidenceBinding(frontier)
	if err != nil || count != 3 || !reflect.DeepEqual(ids, []string{"a", "b"}) || fingerprint == "" {
		t.Fatalf("binding = %d %v %q %v", count, ids, fingerprint, err)
	}
	for _, changed := range []RunForkContractFrontierEvent{
		{SourceEventID: "other", EventName: "work.received"},
		{SourceEventID: "b", EventName: "changed.received"},
	} {
		frontier.FrontierEvents[0] = changed
		_, _, candidate, err := RunForkContractFrontierEvidenceBinding(frontier)
		if err != nil || candidate == fingerprint {
			t.Fatalf("event coordinate mutation did not change binding: %v", err)
		}
	}
}

func TestRunForkContractFrontierEvidenceBindingRejectsInvalidEvidence(t *testing.T) {
	historical := frontierModelHistoricalRoute(t)
	conflictingTarget := historical
	conflictingTarget.Target = events.MustMaterializingEntityTarget(historical.Target.Route())
	conflictingProjection := historical
	var err error
	conflictingProjection.PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"instance": "conflict"})
	if err != nil {
		t.Fatal(err)
	}
	invalidPlan := frontierModelEvidence(t, forkrecipient.Input{Recipient: historical.Recipient, AgentPlan: frontierModelAgentPlan(t), HandlerEvent: "work.received"}, "", "")
	invalidPlan.AgentPlan = agentidentity.Plan{}
	for name, event := range map[string]RunForkContractFrontierEvent{
		"zero_selected":                  {DerivedRecipients: []RunForkContractFrontierRecipient{{}}},
		"invalid_plan":                   {DerivedRecipients: []RunForkContractFrontierRecipient{invalidPlan}},
		"zero_historical":                {HistoricalDeliveryRoutes: []events.DeliveryRoute{{}}},
		"historical_target_conflict":     {HistoricalDeliveryRoutes: []events.DeliveryRoute{historical, conflictingTarget}},
		"historical_projection_conflict": {HistoricalDeliveryRoutes: []events.DeliveryRoute{historical, conflictingProjection}},
	} {
		t.Run(name, func(t *testing.T) {
			count, ids, fingerprint, err := RunForkContractFrontierEvidenceBinding(RunForkContractFrontierAdmission{FrontierEvents: []RunForkContractFrontierEvent{event}})
			if err == nil || count != 0 || ids != nil || fingerprint != "" {
				t.Fatalf("invalid evidence returned usable binding: %d %v %q %v", count, ids, fingerprint, err)
			}
		})
	}
}

func frontierModelFingerprint(t testing.TB, frontier ...RunForkContractFrontierEvent) string {
	t.Helper()
	count, _, fingerprint, err := RunForkContractFrontierEvidenceBinding(RunForkContractFrontierAdmission{FrontierEvents: frontier})
	if err != nil {
		t.Fatal(err)
	}
	if count != len(frontier) || fingerprint == "" {
		t.Fatalf("incomplete binding: count=%d fingerprint=%q", count, fingerprint)
	}
	return fingerprint
}

func frontierModelNodeInput(t testing.TB) forkrecipient.Input {
	t.Helper()
	node := identitytest.FlowNode(t, "review", "worker")
	return forkrecipient.Input{Recipient: events.MustNodeDeliveryRecipient(node), Path: "review/inst-1", HandlerNode: node, HandlerEvent: "work.received", RouteSource: "compiled_connect_evaluation"}
}

func frontierModelEvidence(t testing.TB, input forkrecipient.Input, edge, pin string) RunForkContractFrontierRecipient {
	t.Helper()
	var evidence forkrecipient.Evidence
	var err error
	if edge == "" {
		evidence, err = forkrecipient.NewLocal(input)
	} else {
		// These model tests exercise admitted value/codec identities, not graph admission.
		evidence, err = forkrecipient.NewConnect(input, events.AdmitConnectPlanIdentity(sha256.Sum256([]byte(edge))), events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte(pin))))
	}
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func frontierModelAgentPlan(t testing.TB) agentidentity.Plan {
	t.Helper()
	name, err := agentidentity.DeclaredName("review-agent", "review-owner")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("review", "review-instance", "review/inst-1")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func frontierModelHistoricalRoute(t testing.TB) events.DeliveryRoute {
	t.Helper()
	plan := frontierModelAgentPlan(t)
	live, err := plan.Live("11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"instance": "original"})
	if err != nil {
		t.Fatal(err)
	}
	recipient := events.MustAgentDeliveryRecipient(plan.AgentID())
	return events.DeliveryRoute{Recipient: recipient, AgentIdentity: live,
		Target:  events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1", EntityID: "source-entity"}),
		Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply"}}, PayloadProjection: projection,
		ConnectClaim: frontierModelClaim(t, recipient, "edge", "pin", "work.received"),
	}
}

func frontierModelClaim(t testing.TB, recipient events.DeliveryRecipient, edge, pin, handler string) events.ConnectExecutionClaim {
	t.Helper()
	claim, err := events.AdmitConnectExecutionClaim(sha256.Sum256([]byte(edge)), sha256.Sum256([]byte(pin)), recipient, identity.ExecutableNode{}, events.EventType(handler))
	if err != nil {
		t.Fatal(err)
	}
	return claim
}
