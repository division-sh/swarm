package cliapp

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
)

func TestDescribeTextFactoringCharacterization(t *testing.T) {
	view := describeFactoringView()
	writeDescribeText(nil, view)
	var out bytes.Buffer
	writeDescribeText(&out, view)
	const want = `describe: source=Reception@1.0.0
source authority: projection_only_existing_contract_owners
validation: structural; live readiness: not evaluated
events:
  - root.empty (no fields)
  - root.ready (fields: id, text)
root primary entity: Root
flows:
  - review (template)
    events:
      - ready (fields: id)
    ingress: alias=chat
      - provider=telegram admission=pack-required pack_id=pack authentication=signed event=raw signing_secret=secret.ref
      - provider=webhook admission=intrinsic
    primary entity: Review
    instance: field=id identity=run + flow + instance_key
    singleton coordinator: primary_entity=Coordinator contained_fields=1
    contained operations: 1
  - bare
routing topology: routing-topology/v1
  source authority: projection_only_existing_contract_owners
  routes: none
stage graph:
  flow review (flows/review):
    nodes:
      - open [initial] - Begin
      - done [final]
      - both [initial,final]
      - bare
    edges:
      - open,review -> done (handler receiver on ready after 1s timer deadline loop retry advance max_attempts=3 escape decision approve verdict yes)
      - <none> -> open ()
    timers:
      - open after 1s emit expired advances_to done (timer deadline)
      - bare after 2s
    joins:
      - all stage open members tasks by id output joined deadline 3s from stage_entry until stopped (receiver on result)
      - count stage open members count 2 by id output joined deadline 4s from stage_entry (receiver on result)
      - barrier members from_fan_out (scatter on requested)
      - plain stage bare members tasks by id output joined (receiver on result)
    fan_out:
      - open ->xN item items_from event.items as item identity id max_items 5 (handler receiver on ready)
      - <none> ->xN item items_from event.items
    gates:
      - review decision approve authority=operator reminder=1h draft_ttl=2h
        - yes -> done emit approved
        - no -> open
  flow root:
diagnostics:
  - [WARN] check @ authored.yaml:2: Check this
      remediation: Fix it
      evidence:
        - first
        - second
  - [WARN] fallback @ lowered.yaml:3: Other
`
	if out.String() != want {
		t.Fatalf("describe transcript changed:\n%s\nwant:\n%s", out.String(), want)
	}
	if got := describeQuietValues(view); !reflect.DeepEqual(got, []string{"Reception@1.0.0", "review", "bare"}) {
		t.Fatalf("quiet values=%v", got)
	}
	var empty bytes.Buffer
	writeDescribeText(&empty, authoringview.View{})
	const emptyWant = "describe: source=unavailable\nsource authority: \nvalidation: structural; live readiness: not evaluated\nrouting topology: \n  source authority: \n  routes: none\n"
	if empty.String() != emptyWant {
		t.Fatalf("empty transcript=%q", empty.String())
	}
}

func TestDescribeFactoringRetiredOutputPinFieldsRemainAbsent(t *testing.T) {
	view := describeFactoringView()
	view.Flows[0].OutputPins = []authoringview.OutputPinView{{Event: "ready", ResolvedEvent: "review.ready", BusinessKey: "id", SchemaDigest: "schema", PinDigest: "pin"}}
	data, err := json.Marshal(describeCommandOutput{View: view, ValidationScope: "structural", LiveReadiness: "not_evaluated"})
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}
	pin := output["flows"].([]any)[0].(map[string]any)["output_pins"].([]any)[0].(map[string]any)
	for _, retired := range []string{"key", "carries", "resolution", "resolution_from"} {
		if _, exists := pin[retired]; exists {
			t.Fatalf("retired output field %s reappeared: %s", retired, data)
		}
	}
	if pin["business_key"] != "id" || pin["schema_digest"] != "schema" || pin["resolved_event"] != "review.ready" {
		t.Fatalf("current producer evidence changed: %s", data)
	}
	if output["validation_scope"] != "structural" || output["live_readiness"] != "not_evaluated" || strings.Contains(string(data), "resolution_from") {
		t.Fatalf("structural output changed: %s", data)
	}
}

func TestDescribeFactoringRetiredJoinFieldsRemainAbsent(t *testing.T) {
	data, err := json.Marshal(describeFactoringView())
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}
	joins := output["stage_graphs"].([]any)[0].(map[string]any)["joins"].([]any)
	for _, entry := range joins {
		join := entry.(map[string]any)
		for _, retired := range []string{"members_by_source", "timeout_after", "window_from", "window_by", "window_by_source", "fan_in_pin"} {
			if _, exists := join[retired]; exists {
				t.Fatalf("retired join field %s reappeared: %s", retired, data)
			}
		}
	}
	if explicit := joins[0].(map[string]any); explicit["members_from"] != "tasks" || explicit["deadline_after"] != "3s" || explicit["deadline_from"] != "stage_entry" || explicit["until"] != "stopped" {
		t.Fatalf("explicit membership projection changed: %s", data)
	}
	if count := joins[1].(map[string]any); count["member_count"] != float64(2) || count["deadline_from"] != "stage_entry" {
		t.Fatalf("count membership projection changed: %s", data)
	}
	if barrier := joins[2].(map[string]any); barrier["members_from_fan_out"] != true || barrier["deadline_from"] != nil || barrier["deadline_after"] != nil {
		t.Fatalf("barrier projection changed: %s", data)
	}
}

func describeFactoringView() authoringview.View {
	count := 2
	return authoringview.View{
		SourceHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceLabel: "Reception@1.0.0", SourceAuthority: "projection_only_existing_contract_owners",
		Root: authoringview.RootView{Events: []authoringview.EventView{{Name: "root.empty"}, {Name: "root.ready", Fields: []string{"id", "text"}}}, PrimaryEntity: &authoringview.PrimaryEntityView{Type: "Root"}},
		Flows: []authoringview.FlowView{
			{ID: " review ", Mode: "template", Events: []authoringview.EventView{{Name: "ready", Fields: []string{"id"}}},
				Ingress:       &authoringview.StandingIngressView{Alias: "chat", Providers: []authoringview.StandingIngressProviderView{{Provider: "telegram", AdmissionKind: "pack-required", PackID: "pack", RequestAuthentication: "signed", Event: "raw", SigningSecret: "secret.ref"}, {Provider: "webhook", AdmissionKind: "intrinsic"}}},
				PrimaryEntity: &authoringview.PrimaryEntityView{Type: "Review"}, TemplateInstance: &authoringview.TemplateInstanceView{Field: "id", Identity: "run + flow + instance_key"},
				SingletonCoordinator: &authoringview.SingletonCoordinatorView{PrimaryEntity: "Coordinator", ContainedState: []authoringview.SingletonContainedFieldView{{Name: "items"}}}, ContainedOperations: []authoringview.ContainedOperationView{{Operation: "insert"}}},
			{ID: "bare"},
		},
		RoutingTopology: routingtopology.Topology{SchemaVersion: "routing-topology/v1", SourceAuthority: "projection_only_existing_contract_owners"},
		StageGraphs: []authoringview.StageGraphView{
			{FlowID: " review ", FlowPath: " flows/review ",
				Nodes:  []authoringview.StageGraphNodeView{{ID: "open", Initial: true, Description: " Begin "}, {ID: "done", Final: true}, {ID: "both", Initial: true, Final: true}, {ID: "bare"}},
				Edges:  []authoringview.StageGraphEdgeView{{From: []string{"open", "review"}, To: "done", Source: " handler ", NodeID: " receiver ", EventType: " ready ", After: " 1s ", TimerID: " deadline ", LoopID: "retry", LoopOperation: "advance", MaxAttempts: "3", LoopEscape: true, DecisionID: "approve", Verdict: "yes"}, {To: "open"}},
				Timers: []authoringview.StageGraphTimerView{{Stage: " open ", After: " 1s ", Emit: " expired ", AdvancesTo: " done ", TimerID: " deadline "}, {Stage: "bare", After: "2s"}},
				Joins: []authoringview.StageGraphJoinView{
					{ID: "all", Stage: "open", MembersFrom: "tasks", MembersBy: " id ", Output: "joined", DeadlineAfter: "3s", DeadlineFrom: "stage_entry", Until: "stopped", NodeID: "receiver", HandlerEvent: "result"},
					{ID: "count", Stage: "open", MemberCount: &count, MembersBy: " id ", Output: "joined", DeadlineAfter: "4s", DeadlineFrom: "stage_entry", NodeID: "receiver", HandlerEvent: "result"},
					{ID: "barrier", MembersFromFanOut: true, NodeID: "scatter", HandlerEvent: "requested"},
					{ID: "plain", Stage: "bare", MembersFrom: "tasks", MembersBy: "id", Output: "joined", NodeID: "receiver", HandlerEvent: "result"},
				},
				FanOuts: []authoringview.StageGraphFanOutView{{From: []string{"open"}, Emit: " item ", ItemsFrom: " event.items ", ItemAlias: " item ", Identity: " id ", MaxItems: 5, Source: " handler ", NodeID: " receiver ", EventType: " ready "}, {Emit: "item", ItemsFrom: "event.items"}},
				Gates:   []authoringview.StageGraphGateView{{Stage: "review", Decision: "approve", Authority: "operator", ReminderInterval: "1h", InputDraftTTL: "2h", Outcomes: []authoringview.StageGraphGateOutcomeView{{Verdict: "yes", AdvancesTo: "done", Emit: "approved"}, {Verdict: "no", AdvancesTo: "open"}}}}},
			{},
		},
		Diagnostics: []authoringview.DiagnosticView{{CheckID: "check", Severity: "warning", AuthoredLocation: " authored.yaml:2 ", Location: "ignored", Message: " Check this ", Remediation: " Fix it ", Evidence: []string{" first ", "", "second"}}, {CheckID: "fallback", Severity: "warning", Location: " lowered.yaml:3 ", Message: "Other"}},
	}
}
