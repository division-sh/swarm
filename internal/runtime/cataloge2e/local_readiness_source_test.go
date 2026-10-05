package cataloge2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type localReadinessEntryProbe struct {
	pauseReadinessEntry
	done chan struct{}
	once sync.Once
}

type pauseReadinessEntry struct {
	h    *runtimeHarness
	node string
	once sync.Once
	err  error
}

func (p *pauseReadinessEntry) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerCompleted || signal.SubscriberID != p.node {
		return
	}
	p.once.Do(func() {
		_, p.err = runScopedCatalogStore(p.h.t, p.h).PauseRunControlOutcome(ctx, runcontrol.TransitionRequest{
			RunID: catalogRuntimeRunID, Reason: "pause after connected entry before local frontier", ControlledBy: "cataloge2e",
		})
	})
}

func (p *localReadinessEntryProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerCompleted || signal.SubscriberID != p.node {
		return
	}
	p.pauseReadinessEntry.NotifyLifecycle(ctx, signal)
	p.once.Do(func() { close(p.done) })
}

func publishSelectedForkReadinessFrontier(t *testing.T, ctx context.Context, h *runtimeHarness, eventName string) string {
	t.Helper()
	p := &localReadinessEntryProbe{
		pauseReadinessEntry: pauseReadinessEntry{h: h, node: identitytest.FlowNode(t, "worker-flow", strings.ReplaceAll(eventName, ".", "-")+"-entry").Key()},
		done:                make(chan struct{}),
	}
	h.rt.Pipeline.SetTestLifecycleProbe(p)
	defer h.rt.Pipeline.SetTestLifecycleProbe(nil)
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(eventName), "cataloge2e", "", []byte(`{"worker_id":"worker-001"}`), 0, catalogRuntimeRunID,
		events.EventEnvelope{}, eventtest.RootRoutingSource(catalogRuntimeRunID), time.Now().UTC())
	if err := h.rt.Bus.PublishAcknowledged(ctx, event); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	select {
	case <-p.done:
	case <-deadline.Done():
		t.Fatalf("connected entry did not complete: %v", deadline.Err())
	}
	if p.err != nil {
		t.Fatal(p.err)
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		observed, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		for _, local := range observed {
			if local.EventName != "worker-flow/worker-001/"+eventName || local.SourceEventID != event.ID() {
				continue
			}
			snapshot, err := local.EventSnapshot()
			if err != nil || snapshot.RoutingSource().Route().FlowInstance != "worker-flow/worker-001" || snapshot.ProducerType() != events.EventProducerNode || len(local.Deliveries) == 0 {
				t.Fatalf("local frontier lacks exact producer and consumer: %+v err=%v", local, err)
			}
			for _, delivery := range local.Deliveries {
				if delivery.Status != "pending" {
					t.Fatalf("local frontier was not paused: %+v", delivery)
				}
			}
			return local.EventID
		}
		select {
		case <-deadline.Done():
			t.Fatalf("connected entry did not publish local frontier: %v; events=%+v", deadline.Err(), observed)
		case <-tick.C:
		}
	}
}

// This source uses real creation and node publication, not a private event
// mislabeled as root ingress or a completed dynamic-connection ancestor.
func TestLocalReadinessCreationSourceBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := localReadinessFixture(t, 0, "node")
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			activateLocalReadinessFrontier(t, catalogRunContext(h, catalogRuntimeRunID), h, "worker.inspect")
		})
	}
}

func TestLocalReadinessForkRecipientBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := localReadinessFixture(t, 0, "node")
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			ctx := catalogRunContext(h, catalogRuntimeRunID)
			frontierID := activateLocalReadinessFrontier(t, ctx, h, "worker.inspect")
			plan, err := runScopedCatalogStore(t, h).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: frontierID})
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(h.bundle)
			routes, err := runtimebus.DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := runtimeflowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, runtimeflowidentity.RouteForInstancePath("worker-flow/worker-001"))
			if err != nil {
				t.Fatal(err)
			}
			constructed, found, err := h.workflow.Load(ctx, identity)
			if err != nil || !found {
				t.Fatalf("load constructed local frontier owner: found=%t err=%v", found, err)
			}
			instance, err := constructed.ConstructionIdentity(identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := routes.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
				Identity: identity, Instance: instance,
			}); err != nil {
				t.Fatal(err)
			}
			concrete := routes.ResolveIndependentPubsubForRun(catalogRuntimeRunID, "worker-flow/worker-001/worker.inspect")
			if len(concrete) != 1 {
				t.Fatalf("concrete ordinary route should have one local consumer: %+v", concrete)
			}
			admission, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
			if err != nil {
				t.Fatal(err)
			}
			for _, frontier := range admission.FrontierEvents {
				if frontier.SourceEventID != frontierID {
					continue
				}
				if len(frontier.DerivedRecipients) != 1 || frontier.DerivedRecipients[0].Recipient != concrete[0].Recipient || frontier.DerivedRecipients[0].Path != concrete[0].Path {
					t.Fatalf("selected frontier lost its actual local consumer: ordinary=%+v frontier=%+v blockers=%+v", concrete, frontier, admission.UnsupportedBlockers)
				}
				return
			}
			t.Fatalf("missing local frontier %s: %+v", frontierID, admission)
		})
	}
}

func localReadinessFixture(t *testing.T, declarations int, frontier string) string {
	t.Helper()
	root := selectedForkReadinessCatalogFixture(t, declarations, frontier)
	path := filepath.Join(root, "schema.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := yaml.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	pins := schema["pins"].(map[string]any)
	pins["inputs"] = append(pins["inputs"].([]any), "source.prepare", "source.recorded")
	pins["outputs"] = append(pins["outputs"].([]any), "source.prepare")
	schema["connect"] = append(schema["connect"].([]any), map[string]any{"event": "source.prepare", "from": ".", "to": "worker-flow", "resolution": "select"})
	data, err = yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	event := "worker.inspect"
	if frontier != "node" && !strings.HasPrefix(frontier, "activity") {
		event = "worker.ready"
	}
	for name, addition := range map[string]string{
		"events.yaml":             "\nsource.prepare:\n  worker_id: text\nsource.recorded:\n  worker_id: text\n",
		"nodes.yaml":              "\nprepare:\n  execution_type: system_node\n  subscribes_to: [source.recorded]\n  event_handlers:\n    source.recorded:\n      guard: {id: admitted, check: true}\n",
		"worker-flow/schema.yaml": "\nauto_emit_on_create:\n  event: " + event + ".requested\n",
	} {
		path := filepath.Join(root, name)
		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, addition...), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if strings.HasPrefix(frontier, "activity_loop") {
		// Keep the loop start in the fork frontier. An extra forwarding handler
		// would itself need loop admission and would change that obligation.
		for _, file := range []string{"schema.yaml", "events.yaml", "worker-flow/schema.yaml", "worker-flow/nodes.yaml"} {
			path := filepath.Join(root, file)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			switch file {
			case "schema.yaml":
				var connections []any
				for _, edge := range doc["connect"].([]any) {
					if edge.(map[string]any)["event"] != "worker.inspect" {
						connections = append(connections, edge)
					}
				}
				doc["connect"] = connections
				for _, direction := range []string{"inputs", "outputs"} {
					boundary := doc["pins"].(map[string]any)
					var pins []any
					for _, pin := range boundary[direction].([]any) {
						if pin != "worker.inspect" {
							pins = append(pins, pin)
						}
					}
					boundary[direction] = pins
				}
			case "events.yaml":
				delete(doc, "worker.inspect")
			case "worker-flow/schema.yaml":
				doc["auto_emit_on_create"] = map[string]any{"event": "worker.inspect"}
				boundary := doc["pins"].(map[string]any)
				var pins []any
				for _, pin := range boundary["inputs"].([]any) {
					if pin != "worker.inspect.requested" {
						pins = append(pins, pin)
					}
				}
				boundary["inputs"] = pins
			case "worker-flow/nodes.yaml":
				delete(doc, "worker-inspect-entry")
			}
			raw, err = yaml.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	childSchemaPath := filepath.Join(root, "worker-flow/schema.yaml")
	childSchema, err := os.ReadFile(childSchemaPath)
	if err != nil {
		t.Fatal(err)
	}
	var child map[string]any
	if err := yaml.Unmarshal(childSchema, &child); err != nil {
		t.Fatal(err)
	}
	childPins := child["pins"].(map[string]any)
	childPins["inputs"] = append(childPins["inputs"].([]any), "source.prepare")
	childSchema, err = yaml.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(childSchemaPath, childSchema, 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func activateLocalReadinessFrontier(t *testing.T, ctx context.Context, h *runtimeHarness, eventName string) string {
	t.Helper()
	// Record the causal input without selecting the as-yet unconstructed child.
	// This proof exercises explicit activation, not connect-time construction.
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "source.recorded", "cataloge2e", "", []byte(`{"worker_id":"worker-001"}`), 0, catalogRuntimeRunID,
		events.EventEnvelope{}, eventtest.RootRoutingSource(catalogRuntimeRunID), time.Now().UTC())
	if err := h.rt.Bus.PublishAndWait(ctx, trigger); err != nil {
		t.Fatal(err)
	}
	schema, _ := semanticview.Wrap(h.bundle).FlowSchemaByID("worker-flow")
	directCreation := schema.AutoEmitOnCreate.Event == eventName
	p := &localReadinessEntryProbe{pauseReadinessEntry: pauseReadinessEntry{h: h, node: identitytest.FlowNode(t, "worker-flow", strings.ReplaceAll(eventName, ".", "-")+"-entry").Key()}, done: make(chan struct{})}
	if directCreation {
		if _, err := runScopedCatalogStore(t, h).PauseRunControlOutcome(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "capture loop start creation frontier", ControlledBy: "cataloge2e"}); err != nil {
			t.Fatal(err)
		}
	} else {
		h.rt.Pipeline.SetTestLifecycleProbe(p)
	}
	defer h.rt.Pipeline.SetTestLifecycleProbe(nil)
	entityID := eventtest.UUID("run-scoped-selected-fork-worker")
	if err := h.rt.Manager.ActivateFlowInstance(runtimeeffects.WithExecutionMode(ctx, executionmode.Live), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle:   semanticview.Wrap(h.bundle),
		Instance:         runtimeflowidentity.Stored(semanticview.Wrap(h.bundle), "worker-flow", "worker-flow/worker-001", "worker-001", entityID, ""),
		ConstructorInput: "source.prepare", ResolvedKey: "worker-001",
		TriggerEvent: trigger, OccurredAt: trigger.CreatedAt(),
	}); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if !directCreation {
		select {
		case <-p.done:
		case <-deadline.Done():
			t.Fatalf("creation entry did not complete: %v", deadline.Err())
		}
	}
	if p.err != nil {
		t.Fatal(p.err)
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		observed, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		for _, local := range observed {
			wantName, wantProducer := "worker-flow/worker-001/"+eventName, events.EventProducerNode
			if directCreation {
				wantName, wantProducer = "worker-flow/worker-001/"+eventName, events.EventProducerPlatform
			}
			if local.EventName != wantName {
				continue
			}
			snapshot, err := local.EventSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.ProducerType() != wantProducer || snapshot.RoutingSource().Route().FlowInstance != "worker-flow/worker-001" || len(local.Deliveries) == 0 {
				t.Fatalf("unproven local readiness source: %+v", local)
			}
			for _, delivery := range local.Deliveries {
				if delivery.Status != "pending" {
					t.Fatalf("local frontier not paused: %+v", delivery)
				}
			}
			if directCreation {
				if local.SourceEventID != trigger.ID() || snapshot.Producer().ID() != "flow-instance-activator" {
					t.Fatalf("creation frontier lost activation lineage: %+v", local)
				}
				return local.EventID
			}
			for _, entry := range observed {
				if entry.EventID != local.SourceEventID || entry.SourceEventID != trigger.ID() || entry.EventName != "worker-flow/worker-001/"+eventName+".requested" || len(entry.Deliveries) != 1 {
					continue
				}
				if delivery := entry.Deliveries[0]; delivery.Status == "delivered" && delivery.Terminal {
					return local.EventID
				}
			}
		}
		select {
		case <-deadline.Done():
			t.Fatalf("local frontier or exact creation entry did not settle: %v; events=%+v", deadline.Err(), observed)
		case <-tick.C:
		}
	}
}
