package runforkexecution

import (
	"crypto/sha256"
	"reflect"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractRecipientModelEqualityUsesCanonicalEvidence(t *testing.T) {
	base := recipientModelEqualityInput(t)
	agent := recipientModelEqualityLocal(t, base)
	node := mustRunForkNode("review", "receive")
	nodeInput := forkrecipient.Input{Recipient: events.MustNodeDeliveryRecipient(node), HandlerNode: node, HandlerEvent: "review.ready", Path: "review", RouteSource: "original"}
	localNode := recipientModelEqualityLocal(t, nodeInput)
	connected := recipientModelEqualityConnect(t, nodeInput, "edge-one", "pin-one")
	relabel := base
	relabel.RouteSource = "arbitrary diagnostic, not selected authority"
	diagnostic := recipientModelEqualityLocal(t, relabel)
	variants := map[string]forkrecipient.Evidence{}
	for name, mutate := range map[string]func(*forkrecipient.Input){
		"agent_id": func(in *forkrecipient.Input) {
			in.AgentPlan.Name.AgentID = "other-agent"
			in.Recipient = events.MustAgentDeliveryRecipient("other-agent")
		},
		"plan_owner":     func(in *forkrecipient.Input) { in.AgentPlan.Name.Owner = "other/owner" },
		"plan_source":    func(in *forkrecipient.Input) { in.AgentPlan.Name.Source = agentidentity.NameSourceRuntimeCreated },
		"plan_presence":  func(in *forkrecipient.Input) { in.AgentPlan.Route = agentidentity.RootRoute() },
		"plan_scope":     func(in *forkrecipient.Input) { in.AgentPlan.Route.ScopeKey = "other" },
		"plan_instance":  func(in *forkrecipient.Input) { in.AgentPlan.Route.InstanceID = "other-instance" },
		"plan_path":      func(in *forkrecipient.Input) { in.AgentPlan.Route.InstancePath = "review/other" },
		"recipient_path": func(in *forkrecipient.Input) { in.Path = "review/other" },
		"handler_event":  func(in *forkrecipient.Input) { in.HandlerEvent = "review.changed" },
	} {
		in := base
		mutate(&in)
		variants[name] = recipientModelEqualityLocal(t, in)
	}
	for _, comparator := range recipientModelEqualityComparators() {
		t.Run(comparator.name, func(t *testing.T) {
			for _, label := range []string{"", "stamped_connect_claim", "connect_route_plan", "untrusted"} {
				in := base
				in.RouteSource = label
				requireRecipientModelComparison(t, comparator.compare, []forkrecipient.Evidence{agent}, []forkrecipient.Evidence{recipientModelEqualityLocal(t, in)}, true)
				in = nodeInput
				in.RouteSource = label
				requireRecipientModelComparison(t, comparator.compare, []forkrecipient.Evidence{connected}, []forkrecipient.Evidence{recipientModelEqualityConnect(t, in, "edge-one", "pin-one")}, true)
			}
			requireRecipientModelComparison(t, comparator.compare,
				[]forkrecipient.Evidence{agent, localNode, connected},
				[]forkrecipient.Evidence{connected, localNode, diagnostic, agent, localNode}, true)
			requireRecipientModelComparison(t, comparator.compare, nil, []forkrecipient.Evidence{}, true)
			for name, other := range variants {
				t.Run(name, func(t *testing.T) {
					requireRecipientModelComparison(t, comparator.compare, []forkrecipient.Evidence{agent}, []forkrecipient.Evidence{other}, false)
					// Same-ID distinct plans must not disappear as duplicates.
					requireRecipientModelComparison(t, comparator.compare, []forkrecipient.Evidence{agent, other}, []forkrecipient.Evidence{agent}, false)
				})
			}
			for name, other := range map[string]forkrecipient.Evidence{
				"edge":  recipientModelEqualityConnect(t, nodeInput, "edge-two", "pin-one"),
				"pin":   recipientModelEqualityConnect(t, nodeInput, "edge-one", "pin-two"),
				"local": localNode,
			} {
				t.Run(name, func(t *testing.T) {
					requireRecipientModelComparison(t, comparator.compare, []forkrecipient.Evidence{connected}, []forkrecipient.Evidence{other}, false)
				})
			}
			otherNode := mustRunForkNode("review", "other-handler")
			otherNodeInput := nodeInput
			otherNodeInput.Recipient, otherNodeInput.HandlerNode = events.MustNodeDeliveryRecipient(otherNode), otherNode
			requireRecipientModelComparison(t, comparator.compare, []forkrecipient.Evidence{localNode}, []forkrecipient.Evidence{recipientModelEqualityLocal(t, otherNodeInput)}, false)
		})
	}
}

func TestSelectedContractRecipientModelEqualityRejectsMalformedEvidence(t *testing.T) {
	valid := recipientModelEqualityLocal(t, recipientModelEqualityInput(t))
	invalidPlan := valid
	invalidPlan.AgentPlan.Name.Owner = ""
	for _, comparator := range recipientModelEqualityComparators() {
		t.Run(comparator.name, func(t *testing.T) {
			for _, malformed := range []forkrecipient.Evidence{{}, invalidPlan} {
				for _, pair := range [][2][]forkrecipient.Evidence{
					{{valid}, {malformed}},
					{{malformed}, {valid}},
					{{valid, malformed, valid}, {valid}},
					{nil, {malformed}},
					{{malformed}, {malformed}},
				} {
					if equal, err := comparator.compare(pair[0], pair[1]); err == nil || equal {
						t.Fatalf("malformed recipient became equality/mismatch instead of error: equal=%v err=%v", equal, err)
					}
				}
			}
		})
	}
	left := recipientModelEqualityPlanning([]forkrecipient.Evidence{valid})
	right := recipientModelEqualityPlanning([]forkrecipient.Evidence{valid})
	right.RecipientPlanEvents = append(right.RecipientPlanEvents, runfork.RunForkSelectedContractRecipientPlanEvent{Recipients: []forkrecipient.Evidence{{}}})
	if equal, err := runfork.EqualSelectedContractRecipientPlanning(left, right); err == nil || equal {
		t.Fatalf("event cardinality mismatch hid malformed evidence: %v/%v", equal, err)
	}
}

func TestSelectedContractRecipientModelEqualityKeepsMetadataExact(t *testing.T) {
	evidence := []forkrecipient.Evidence{recipientModelEqualityLocal(t, recipientModelEqualityInput(t))}
	for name, mutate := range map[string]func(*runfork.RunForkSelectedContractRecipientPlanning){
		"owner":          func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.Owner = "other" },
		"route_owner":    func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.RouteTopologyOwner = "other" },
		"mutating":       func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.NonMutating = false },
		"supported":      func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.RecipientPlanningSupported = false },
		"delivery_write": func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.DeliveryWritesSupported = true },
		"selection":      func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.ContractSelection.BundleHash = "other" },
		"frontier_hash":  func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.FrontierEvidenceFingerprint = "other" },
		"frontier_count": func(p *runfork.RunForkSelectedContractRecipientPlanning) { p.FrontierEventCount++ },
		"source_event": func(p *runfork.RunForkSelectedContractRecipientPlanning) {
			p.RecipientPlanEvents[0].SourceEventID = "other"
		},
		"event_name": func(p *runfork.RunForkSelectedContractRecipientPlanning) {
			p.RecipientPlanEvents[0].EventName = "other.event"
		},
		"disposition": func(p *runfork.RunForkSelectedContractRecipientPlanning) {
			p.RecipientPlanEvents[0].Disposition = "other"
		},
		"blockers": func(p *runfork.RunForkSelectedContractRecipientPlanning) {
			p.UnsupportedBlockers = []runfork.RunForkUnsupportedBlocker{{Code: "blocked"}}
		},
		"consumer": func(p *runfork.RunForkSelectedContractRecipientPlanning) {
			p.RequiredConsumers = []runfork.RunForkSelectedContractExecutionBoundary{{Owner: "other"}}
		},
	} {
		t.Run("planning/"+name, func(t *testing.T) {
			left, right := recipientModelEqualityPlanning(evidence), recipientModelEqualityPlanning(evidence)
			mutate(&right)
			if equal, err := runfork.EqualSelectedContractRecipientPlanning(left, right); err != nil || equal {
				t.Fatalf("changed metadata accepted: %v/%v", equal, err)
			}
		})
	}
	for name, mutate := range map[string]func(*runfork.RunForkSelectedContractRouteTopology){
		"owner":         func(p *runfork.RunForkSelectedContractRouteTopology) { p.Owner = "other" },
		"executable":    func(p *runfork.RunForkSelectedContractRouteTopology) { p.ExecutableRecipientsSupported = true },
		"persistence":   func(p *runfork.RunForkSelectedContractRouteTopology) { p.RoutePersistenceSupported = true },
		"dynamic_owner": func(p *runfork.RunForkSelectedContractRouteTopology) { p.DynamicTopologyOwner = "other" },
		"selection":     func(p *runfork.RunForkSelectedContractRouteTopology) { p.ContractSelection.Mode = "other" },
		"static_event": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.StaticRouteEvents[0].EventName = "other.event"
		},
		"static_source": func(p *runfork.RunForkSelectedContractRouteTopology) { p.StaticRouteEvents[0].SourceEventID = "other" },
		"historical_route": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.StaticRouteEvents[0].HistoricalDeliveryRoutes = []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient("historical")}}
		},
		"dynamic_flow": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.DynamicTopologyProofs[0].FlowInstance = "other"
		},
		"dynamic_events": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.DynamicTopologyProofs[0].EventNames = []string{"other.event"}
		},
		"dynamic_source": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.DynamicTopologyProofs[0].SourceEventIDs = []string{"other"}
		},
		"dynamic_status": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.DynamicTopologyProofs[0].Disposition = "other"
		},
		"event_groups": func(p *runfork.RunForkSelectedContractRouteTopology) {
			p.StaticRouteEvents = append(p.StaticRouteEvents, p.StaticRouteEvents[0])
			p.DynamicTopologyProofs = nil
		},
	} {
		t.Run("topology/"+name, func(t *testing.T) {
			left, right := recipientModelEqualityTopology(evidence), recipientModelEqualityTopology(evidence)
			mutate(&right)
			if equal, err := runfork.EqualSelectedContractRouteTopology(left, right); err != nil || equal {
				t.Fatalf("changed metadata accepted: %v/%v", equal, err)
			}
		})
	}
	for name, mutate := range map[string]func(*runfork.RunForkSelectedContractFrontierEvent){
		"source":      func(e *runfork.RunForkSelectedContractFrontierEvent) { e.SourceEventID = "other" },
		"event":       func(e *runfork.RunForkSelectedContractFrontierEvent) { e.EventName = "other.event" },
		"owner":       func(e *runfork.RunForkSelectedContractFrontierEvent) { e.RuntimeEventOwners = []string{"other"} },
		"subscriber":  func(e *runfork.RunForkSelectedContractFrontierEvent) { e.WorkflowNodeSubscribers = []string{"other"} },
		"disposition": func(e *runfork.RunForkSelectedContractFrontierEvent) { e.Disposition = "other" },
	} {
		t.Run("frontier/"+name, func(t *testing.T) {
			left, right := recipientModelEqualityFrontier(evidence), recipientModelEqualityFrontier(evidence)
			mutate(&right[0])
			if equal, err := runfork.EqualSelectedContractFrontierEvents(left, right); err != nil || equal {
				t.Fatalf("changed metadata accepted: %v/%v", equal, err)
			}
		})
	}
}

func TestSelectedContractRecipientModelEqualityDoesNotMutateCarriers(t *testing.T) {
	evidence := []forkrecipient.Evidence{recipientModelEqualityLocal(t, recipientModelEqualityInput(t))}
	planning := recipientModelEqualityPlanning(evidence)
	topology := recipientModelEqualityTopology(evidence)
	frontier := recipientModelEqualityFrontier(evidence)
	for iteration := 0; iteration < 3; iteration++ {
		if equal, err := runfork.EqualSelectedContractRecipientPlanning(planning, planning); err != nil || !equal {
			t.Fatalf("self planning: %v/%v", equal, err)
		}
		if equal, err := runfork.EqualSelectedContractRouteTopology(topology, topology); err != nil || !equal {
			t.Fatalf("self topology: %v/%v", equal, err)
		}
		if equal, err := runfork.EqualSelectedContractFrontierEvents(frontier, frontier); err != nil || !equal {
			t.Fatalf("self frontier: %v/%v", equal, err)
		}
	}
	if !reflect.DeepEqual(planning, recipientModelEqualityPlanning(evidence)) ||
		!reflect.DeepEqual(topology, recipientModelEqualityTopology(evidence)) ||
		!reflect.DeepEqual(frontier, recipientModelEqualityFrontier(evidence)) {
		t.Fatal("comparison mutated a caller's aliased carrier slices")
	}
}

func TestSelectedContractRecipientModelEqualityValidatorEntrypoints(t *testing.T) {
	base := recipientModelEqualityInput(t)
	for _, carrier := range []string{"topology_static", "topology_dynamic", "planning", "model_frontier", "model_topology_static", "model_topology_dynamic", "model_planning"} {
		for _, change := range []string{"unchanged", "diagnostic_only", "plan_owner", "handler_event", "malformed", "metadata"} {
			t.Run(carrier+"/"+change, func(t *testing.T) {
				f := newRecipientModelEqualityValidatorFixture(t)
				var recipients *[]forkrecipient.Evidence
				var eventName *string
				var validate func() error
				switch carrier {
				case "topology_static", "topology_dynamic":
					if carrier == "topology_static" {
						recipients = &f.topology.StaticRouteEvents[0].DerivedRecipients
						eventName = &f.topology.StaticRouteEvents[0].EventName
					} else {
						recipients = &f.topology.DynamicTopologyProofs[0].DerivedRecipients
						eventName = &f.topology.DynamicTopologyProofs[0].EventNames[0]
					}
					validate = func() error {
						return validateSelectedContractRouteTopology(f.frontier, f.routeAdmission, f.topology)
					}
				case "planning":
					recipients = &f.model.RecipientPlanning.RecipientPlanEvents[0].Recipients
					eventName = &f.model.RecipientPlanning.RecipientPlanEvents[0].EventName
					validate = func() error {
						return validateSelectedContractRecipientPlanning(f.frontier, f.routeAdmission, f.topology, *f.model.RecipientPlanning)
					}
				default:
					switch carrier {
					case "model_frontier":
						recipients = &f.model.FrontierEvents[0].DerivedRecipients
						eventName = &f.model.FrontierEvents[0].EventName
					case "model_topology_static":
						recipients = &f.model.RouteTopology.StaticRouteEvents[0].DerivedRecipients
						eventName = &f.model.RouteTopology.StaticRouteEvents[0].EventName
					case "model_topology_dynamic":
						recipients = &f.model.RouteTopology.DynamicTopologyProofs[0].DerivedRecipients
						eventName = &f.model.RouteTopology.DynamicTopologyProofs[0].EventNames[0]
					case "model_planning":
						recipients = &f.model.RecipientPlanning.RecipientPlanEvents[0].Recipients
						eventName = &f.model.RecipientPlanning.RecipientPlanEvents[0].EventName
					}
					validate = func() error {
						return validateSelectedContractExecutionModel(f.binding, f.frontier, f.routeAdmission, f.topology, f.model)
					}
				}
				if err := validate(); err != nil {
					t.Fatalf("unmodified canonical model rejected: %v", err)
				}
				in := base
				switch change {
				case "diagnostic_only":
					in.RouteSource = "independently restored diagnostic"
				case "plan_owner":
					in.AgentPlan.Name.Owner = "other/owner"
				case "handler_event":
					in.HandlerEvent = "other.event"
				case "metadata":
					*eventName = "other.event"
				}
				if change == "malformed" {
					*recipients = []forkrecipient.Evidence{{}}
				} else {
					// Replace only this carrier, never its canonical comparator input.
					*recipients = []forkrecipient.Evidence{recipientModelEqualityLocal(t, in)}
				}
				wantOK := change == "unchanged" || change == "diagnostic_only"
				if err := validate(); (err == nil) != wantOK {
					t.Fatalf("validator error = %v, want acceptance %v", err, wantOK)
				}
			})
		}
	}
}

type recipientModelEqualityValidatorFixture struct {
	frontier       runfork.RunForkContractFrontierAdmission
	routeAdmission runfork.RunForkSelectedContractRouteAdmission
	topology       runfork.RunForkSelectedContractRouteTopology
	model          runfork.RunForkSelectedContractExecution
	binding        runfork.RunForkSelectedContractBinding
}

func newRecipientModelEqualityValidatorFixture(t *testing.T) recipientModelEqualityValidatorFixture {
	t.Helper()
	// Exercise model-consumption validators, not source loading or live admission.
	evidence := recipientModelEqualityLocal(t, recipientModelEqualityInput(t))
	f := recipientModelEqualityValidatorFixture{
		frontier: runfork.RunForkContractFrontierAdmission{
			Owner: runfork.RunForkContractFrontierAdmissionOwner, NonMutating: true,
			ContractSelection:  runfork.RunForkContractSelection{Mode: "selected_contracts"},
			FrontierEventCount: 1,
			FrontierEvents: []runfork.RunForkContractFrontierEvent{{
				SourceEventID: "event", EventName: "review.ready", SourceFlowInstances: []string{"review/instance"},
				DerivedRecipients: []forkrecipient.Evidence{evidence},
			}},
		},
	}
	f.routeAdmission = testSelectedContractRouteAdmission(f.frontier)
	f.routeAdmission.DynamicFlowInstances = []string{"review/instance"}
	f.routeAdmission.SelectedRouteEvents = []runfork.RunForkSelectedContractRouteEvent{{
		SourceEventID: "event", EventName: "review.ready", DerivedRecipients: []forkrecipient.Evidence{evidence},
		Disposition: runfork.RunForkSelectedContractDispositionEvidenceOnly,
	}}
	buildTopology := func() runfork.RunForkSelectedContractRouteTopology {
		topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{Admission: f.frontier, RouteAdmission: f.routeAdmission})
		if err != nil {
			t.Fatal(err)
		}
		if !topology.DynamicTopologySupported || len(topology.DynamicTopologyProofs) != 1 || len(topology.StaticRouteEvents) != 1 {
			t.Fatalf("fixture lacks static/dynamic proof: %+v", topology)
		}
		return topology
	}
	f.topology = buildTopology()
	// Build independently so candidate mutations cannot change expected topology.
	var err error
	f.model, err = BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission: f.frontier, RouteAdmission: f.routeAdmission, RouteTopology: buildTopology(),
	})
	if err != nil {
		t.Fatal(err)
	}
	f.binding = runfork.RunForkSelectedContractBinding{
		Owner: runfork.RunForkSelectedContractBindingOwner, ContractSelection: f.frontier.ContractSelection,
	}
	return f
}

type recipientModelEqualityComparator struct {
	name    string
	compare func([]forkrecipient.Evidence, []forkrecipient.Evidence) (bool, error)
}

func recipientModelEqualityComparators() []recipientModelEqualityComparator {
	return []recipientModelEqualityComparator{
		{"planning", func(l, r []forkrecipient.Evidence) (bool, error) {
			return runfork.EqualSelectedContractRecipientPlanning(recipientModelEqualityPlanning(l), recipientModelEqualityPlanning(r))
		}},
		{"static_topology", func(l, r []forkrecipient.Evidence) (bool, error) {
			left, right := recipientModelEqualityTopology(l), recipientModelEqualityTopology(r)
			left.DynamicTopologyProofs, right.DynamicTopologyProofs = nil, nil
			return runfork.EqualSelectedContractRouteTopology(left, right)
		}},
		{"dynamic_topology", func(l, r []forkrecipient.Evidence) (bool, error) {
			left, right := recipientModelEqualityTopology(l), recipientModelEqualityTopology(r)
			left.StaticRouteEvents, right.StaticRouteEvents = nil, nil
			return runfork.EqualSelectedContractRouteTopology(left, right)
		}},
		{"frontier", func(l, r []forkrecipient.Evidence) (bool, error) {
			return runfork.EqualSelectedContractFrontierEvents(recipientModelEqualityFrontier(l), recipientModelEqualityFrontier(r))
		}},
	}
}

func requireRecipientModelComparison(t *testing.T, compare func([]forkrecipient.Evidence, []forkrecipient.Evidence) (bool, error), left, right []forkrecipient.Evidence, want bool) {
	t.Helper()
	beforeLeft, beforeRight := slices.Clone(left), slices.Clone(right)
	for _, pair := range [][2][]forkrecipient.Evidence{{left, right}, {right, left}} {
		if got, err := compare(pair[0], pair[1]); err != nil || got != want {
			t.Fatalf("semantic comparison = %v/%v, want %v", got, err, want)
		}
	}
	if !reflect.DeepEqual(left, beforeLeft) || !reflect.DeepEqual(right, beforeRight) {
		t.Fatal("comparison mutated evidence ordering or values")
	}
}

func recipientModelEqualityInput(t *testing.T) forkrecipient.Input {
	t.Helper()
	name, err := agentidentity.DeclaredName("reviewer", "selected/review")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("review", "instance", "review/instance")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	return forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient("reviewer"), Path: "review/instance", AgentPlan: plan, HandlerEvent: "review.ready", RouteSource: "original"}
}

func recipientModelEqualityLocal(t *testing.T, in forkrecipient.Input) forkrecipient.Evidence {
	t.Helper()
	e, err := forkrecipient.NewLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func recipientModelEqualityConnect(t *testing.T, in forkrecipient.Input, edge, pin string) forkrecipient.Evidence {
	t.Helper()
	// Pure DTO equality, not compiled graph membership or execution admission.
	e, err := forkrecipient.NewConnect(in, events.AdmitConnectPlanIdentity(sha256.Sum256([]byte(edge))), events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte(pin))))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func recipientModelEqualityPlanning(e []forkrecipient.Evidence) runfork.RunForkSelectedContractRecipientPlanning {
	return runfork.RunForkSelectedContractRecipientPlanning{
		Owner: runfork.RunForkSelectedContractRecipientPlanningOwner, RouteTopologyOwner: runfork.RunForkSelectedContractRouteTopologyOwner,
		NonMutating: true, RecipientPlanningSupported: true,
		ContractSelection:  runfork.RunForkContractSelection{Mode: "selected_contracts"},
		FrontierEventCount: 1, FrontierEvidenceFingerprint: "frontier",
		RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{SourceEventID: "event", EventName: "review.ready", Disposition: "selected", Recipients: e}},
	}
}

func recipientModelEqualityTopology(e []forkrecipient.Evidence) runfork.RunForkSelectedContractRouteTopology {
	return runfork.RunForkSelectedContractRouteTopology{
		Owner: runfork.RunForkSelectedContractRouteTopologyOwner, NonMutating: true,
		ContractSelection:     runfork.RunForkContractSelection{Mode: "selected_contracts"},
		StaticRouteEvents:     []runfork.RunForkSelectedContractRouteEvent{{SourceEventID: "event", EventName: "review.ready", Disposition: "selected", DerivedRecipients: e}},
		DynamicTopologyProofs: []runfork.RunForkSelectedContractDynamicTopologyProof{{FlowInstance: "review/instance", SourceEventIDs: []string{"event"}, EventNames: []string{"review.ready"}, Disposition: "selected", DerivedRecipients: e}},
	}
}

func recipientModelEqualityFrontier(e []forkrecipient.Evidence) []runfork.RunForkSelectedContractFrontierEvent {
	return []runfork.RunForkSelectedContractFrontierEvent{{SourceEventID: "event", EventName: "review.ready", Disposition: "evidence", DerivedRecipients: e}}
}
