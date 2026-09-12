package forkrecipient

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func nodeInput(t *testing.T) Input {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration("review", "receive")
	if err != nil {
		t.Fatal(err)
	}
	return Input{
		Recipient: events.MustNodeDeliveryRecipient(node), Path: "review",
		HandlerNode: node, HandlerEvent: "review.requested", RouteSource: "selected",
	}
}

func agentInput(t *testing.T) Input {
	t.Helper()
	name, err := agentidentity.DeclaredName("reviewer", "pack-a/review")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("review", "case-a", "review/case-a")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	return Input{
		Recipient: events.MustAgentDeliveryRecipient(plan.AgentID()), Path: "review/case-a",
		AgentPlan: plan, HandlerEvent: "review.requested", RouteSource: "selected",
	}
}

func localEvidence(t *testing.T, in Input) Evidence {
	t.Helper()
	e, err := NewLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func connectEvidence(t *testing.T, in Input, plan, pin string) Evidence {
	t.Helper()
	// Typed digest values exercise this pure owner's laws, not graph admission.
	e, err := NewConnect(in,
		events.AdmitConnectPlanIdentity(sha256.Sum256([]byte(plan))),
		events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte(pin))),
	)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func evidenceVariants(t *testing.T) map[string]Evidence {
	t.Helper()
	base := agentInput(t)
	out := map[string]Evidence{"base": localEvidence(t, base)}
	mutations := map[string]func(*Input){
		"agent_id": func(in *Input) {
			in.AgentPlan.Name.AgentID = "other"
			in.Recipient = events.MustAgentDeliveryRecipient("other")
		},
		"name_owner":     func(in *Input) { in.AgentPlan.Name.Owner = "pack-b/review" },
		"name_source":    func(in *Input) { in.AgentPlan.Name.Source = agentidentity.NameSourceRuntimeCreated },
		"route_presence": func(in *Input) { in.AgentPlan.Route = agentidentity.RootRoute() },
		"route_scope":    func(in *Input) { in.AgentPlan.Route.ScopeKey = "other" },
		"route_instance": func(in *Input) { in.AgentPlan.Route.InstanceID = "case-b" },
		"route_path":     func(in *Input) { in.AgentPlan.Route.InstancePath = "review/case-b" },
		"recipient_path": func(in *Input) { in.Path = "other/case-a" },
		"handler_event":  func(in *Input) { in.HandlerEvent = "review.ready" },
	}
	for name, mutate := range mutations {
		in := base
		mutate(&in)
		out[name] = localEvidence(t, in)
	}
	out["connect"] = connectEvidence(t, base, "plan-a", "pin-a")
	out["connect_plan"] = connectEvidence(t, base, "plan-b", "pin-a")
	out["connect_pin"] = connectEvidence(t, base, "plan-a", "pin-b")
	node := nodeInput(t)
	out["node"] = localEvidence(t, node)
	other, err := identity.AdmitExecutableNodeDeclaration("review", "other")
	if err != nil {
		t.Fatal(err)
	}
	node.Recipient, node.HandlerNode = events.MustNodeDeliveryRecipient(other), other
	out["node_declaration"] = localEvidence(t, node)
	return out
}

func TestSelectedEvidenceIdentityOrderAndFingerprintLaws(t *testing.T) {
	variants := evidenceVariants(t)
	names := make([]string, 0, len(variants))
	for name := range variants {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, leftName := range names {
		left := variants[leftName]
		for _, rightName := range names {
			right := variants[rightName]
			t.Run(leftName+"/"+rightName, func(t *testing.T) {
				equal, err := Equal(left, right)
				if err != nil || equal != (leftName == rightName) {
					t.Fatalf("Equal = %v, %v", equal, err)
				}
				leftKey, err := left.Key()
				if err != nil {
					t.Fatal(err)
				}
				rightKey, err := right.Key()
				if err != nil || (leftKey == rightKey) != equal {
					t.Fatalf("key/equality disagreement: %v", err)
				}
				leftHash, err := left.Fingerprint()
				if err != nil {
					t.Fatal(err)
				}
				rightHash, err := right.Fingerprint()
				if err != nil || (leftHash == rightHash) != equal {
					t.Fatalf("fingerprint/equality disagreement: %v", err)
				}
				less, err := Less(left, right)
				if err != nil {
					t.Fatal(err)
				}
				reverse, err := Less(right, left)
				if err != nil || (equal && (less || reverse)) || (!equal && less == reverse) {
					t.Fatalf("not a strict total order: less=%v reverse=%v err=%v", less, reverse, err)
				}
				for _, thirdName := range names {
					third := variants[thirdName]
					rightLessThird, err := Less(right, third)
					if err != nil {
						t.Fatal(err)
					}
					leftLessThird, err := Less(left, third)
					if err != nil || (less && rightLessThird && !leftLessThird) {
						t.Fatalf("order is not transitive through %s: %v", thirdName, err)
					}
				}
			})
		}
	}
}

func TestSelectedEvidenceDiagnosticsAreNotSemanticIdentity(t *testing.T) {
	in := agentInput(t)
	base := localEvidence(t, in)
	for _, diagnostic := range []string{"", "stamped_connect_claim", "connect_route_plan", "misleading"} {
		in.RouteSource = diagnostic
		other := localEvidence(t, in)
		equal, err := Equal(base, other)
		if err != nil || !equal {
			t.Fatalf("diagnostic %q changed identity: %v", diagnostic, err)
		}
		baseHash, _ := base.Fingerprint()
		otherHash, _ := other.Fingerprint()
		if baseHash != otherHash {
			t.Fatalf("diagnostic %q changed fingerprint", diagnostic)
		}
		if less, err := Less(base, other); err != nil || less {
			t.Fatalf("diagnostic changed ordering: %v", err)
		}
		if other.RouteSourceCode() != diagnostic {
			t.Fatal("diagnostic was not preserved for readback")
		}
	}
	in = agentInput(t)
	in.Path = " " + in.Path + " "
	in.AgentPlan.Name.Owner = " " + in.AgentPlan.Name.Owner + " "
	in.AgentPlan.Route.ScopeKey = "/review/"
	normalized := localEvidence(t, in)
	if equal, err := Equal(base, normalized); err != nil || !equal {
		t.Fatalf("canonical agent normalization disagrees: %v", err)
	}
}

func TestSelectedEvidenceCanonicalSetValidatesAndDeduplicates(t *testing.T) {
	variants := evidenceVariants(t)
	input := make([]Evidence, 0, len(variants)+1)
	for _, e := range variants {
		input = append(input, e)
	}
	diagnostic := variants["base"]
	diagnostic.routeSource = "another"
	input = append(input, diagnostic)
	want, err := CanonicalSet(input)
	if err != nil || len(want) != len(variants) {
		t.Fatalf("CanonicalSet count=%d err=%v", len(want), err)
	}
	for rotation := 0; rotation < len(input); rotation++ {
		permuted := append(slices.Clone(input[rotation:]), input[:rotation]...)
		if rotation%2 == 0 {
			slices.Reverse(permuted)
		}
		before := slices.Clone(permuted)
		got, err := CanonicalSet(permuted)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("permutation %d changed canonical set: %v", rotation, err)
		}
		if !reflect.DeepEqual(permuted, before) {
			t.Fatal("CanonicalSet mutated its input")
		}
	}
	twice, err := CanonicalSet(want)
	if err != nil || !reflect.DeepEqual(twice, want) {
		t.Fatalf("CanonicalSet is not idempotent: %v", err)
	}
	for _, in := range [][]Evidence{nil, {}} {
		got, err := CanonicalSet(in)
		if err != nil || got != nil {
			t.Fatalf("empty set = %#v, %v", got, err)
		}
	}
	for _, in := range [][]Evidence{
		{variants["base"], {}, variants["base"]},
		{{}, variants["base"]},
	} {
		if got, err := CanonicalSet(in); err == nil || got != nil {
			t.Fatalf("invalid member disappeared: %#v, %v", got, err)
		}
	}
}

func TestSelectedEvidenceRejectsIncompleteAndContradictoryInputs(t *testing.T) {
	base := nodeInput(t)
	tests := map[string]Input{
		"empty":           {},
		"missing_handler": {Recipient: base.Recipient, HandlerEvent: base.HandlerEvent},
		"missing_event":   {Recipient: base.Recipient, HandlerNode: base.HandlerNode},
	}
	for name, mutate := range map[string]func(*Input){
		"node_with_plan": func(in *Input) { in.AgentPlan = agentInput(t).AgentPlan },
		"wrong_handler": func(in *Input) {
			in.HandlerNode, _ = identity.AdmitExecutableNodeDeclaration("review", "other")
		},
		"pattern_event": func(in *Input) { in.HandlerEvent = "review.*" },
		"aliased_event": func(in *Input) { in.HandlerEvent = "/review.requested/" },
		"invalid_path":  func(in *Input) { in.Path = "review\x00other" },
	} {
		in := base
		mutate(&in)
		tests[name] = in
	}
	for name, mutate := range map[string]func(*Input){
		"agent_without_plan": func(in *Input) { in.AgentPlan = agentidentity.Plan{} },
		"agent_wrong_id":     func(in *Input) { in.AgentPlan.Name.AgentID = "other" },
		"agent_with_node":    func(in *Input) { in.HandlerNode = base.HandlerNode },
		"agent_bad_route":    func(in *Input) { in.AgentPlan.Route.InstanceID = "" },
	} {
		in := agentInput(t)
		mutate(&in)
		tests[name] = in
	}
	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			if got, err := NewLocal(in); err == nil || got != (Evidence{}) {
				t.Fatalf("accepted invalid input: %#v, %v", got, err)
			}
		})
	}
	connected := connectEvidence(t, base, "plan", "pin")
	for _, corrupt := range []func(*Evidence){
		func(e *Evidence) { e.authority = 0 },
		func(e *Evidence) { e.authority = authorityKind(255) },
		func(e *Evidence) { e.authority = authorityLocal },
		func(e *Evidence) { e.connectPlan = events.ConnectPlanIdentity{} },
		func(e *Evidence) { e.receiverPin = events.ConnectReceiverIdentity{} },
	} {
		e := connected
		corrupt(&e)
		if err := e.Validate(); err == nil {
			t.Fatal("accepted invalid authority variant")
		}
		if _, err := e.Key(); err == nil {
			t.Fatal("invalid authority acquired a key")
		}
		if _, err := e.Fingerprint(); err == nil {
			t.Fatal("invalid authority acquired a fingerprint")
		}
		if _, err := json.Marshal(e); err == nil {
			t.Fatal("invalid authority serialized")
		}
		for _, pair := range [][2]Evidence{{e, connected}, {connected, e}} {
			if _, err := Equal(pair[0], pair[1]); err == nil {
				t.Fatal("invalid authority compared equal")
			}
			if _, err := Less(pair[0], pair[1]); err == nil {
				t.Fatal("invalid authority acquired an order")
			}
		}
	}
}

func TestSelectedEvidenceSatisfactionRequiresExactChildPlanAndRun(t *testing.T) {
	const childRun = "8ad02a4e-aa23-43b2-9982-90e8335f1c5f"
	const sourceRun = "9da839ff-5556-4d9a-bc99-d407edc60f1e"
	const siblingRun = "677d0aa8-569e-4d04-8bb5-0521ac802172"
	selected := localEvidence(t, agentInput(t))
	for _, run := range []string{childRun, sourceRun, siblingRun} {
		live, err := selected.AgentPlan.Live(run)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := live.Plan()
		if err != nil || plan != selected.AgentPlan {
			t.Fatalf("lawful composition lost plan: %v", err)
		}
		err = selected.Satisfies(childRun, selected, live)
		if (err == nil) != (run == childRun) {
			t.Fatalf("run %q satisfaction: %v", run, err)
		}
	}
	live, err := selected.AgentPlan.Live(childRun)
	if err != nil {
		t.Fatal(err)
	}
	for name, actual := range evidenceVariants(t) {
		err := selected.Satisfies(childRun, actual, live)
		if (err == nil) != (name == "base") {
			t.Fatalf("actual %s satisfaction: %v", name, err)
		}
		if actual.Recipient.IsAgent() && name != "base" {
			wrongLive, err := actual.AgentPlan.Live(childRun)
			if err != nil {
				t.Fatal(err)
			}
			if actual.AgentPlan != selected.AgentPlan && selected.Satisfies(childRun, selected, wrongLive) == nil {
				t.Fatalf("actual live plan %s satisfied different selected plan", name)
			}
		}
	}
	diagnostic := selected
	diagnostic.routeSource = "stamped_connect_claim"
	if err := selected.Satisfies(childRun, diagnostic, live); err != nil {
		t.Fatalf("diagnostic changed satisfaction: %v", err)
	}
	for _, run := range []string{"", "  "} {
		if err := selected.Satisfies(run, selected, live); err == nil {
			t.Fatal("missing child run admitted")
		}
	}
	if err := selected.Satisfies(childRun, selected, agentidentity.Identity{}); err == nil {
		t.Fatal("runless blueprint substituted for actual live identity")
	}
	node := localEvidence(t, nodeInput(t))
	if err := node.Satisfies(childRun, node, agentidentity.Identity{}); err != nil {
		t.Fatalf("node without agent subset: %v", err)
	}
	if node.Satisfies(childRun, node, live) == nil || node.Satisfies("", node, agentidentity.Identity{}) == nil {
		t.Fatal("node accepted foreign agent authority or missing child binding")
	}
}
