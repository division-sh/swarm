package builder

import (
	"context"
	"net/http"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type runStartAppendStore struct {
	appended []string
}

func (s *runStartAppendStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublish(ctx, plan, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		s.appended = append(s.appended, string(req.Event.Event().Type()))
		return nil
	})
}

func (*runStartAppendStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func (*runStartAppendStore) SupportsPersistedReplay() bool { return false }

func TestHandlerRunStartRejectsUndeclaredInputBeforePublish(t *testing.T) {
	source := semanticview.Wrap(runStartInputBundle("scan.corpus_file_requested"))
	store := &runStartAppendStore{}
	_, acquirer := newTestOwnedEventBus(t, store, runtimebus.EventBusOptions{ContractBundle: source})
	handler := NewHandler(Options{
		AuthToken:       testBuilderAuthToken,
		SemanticSource:  source,
		RuntimeAcquirer: acquirer,
	})

	resp := callBuilderRPCRaw(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "reject",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": "run-123",
			"inputs": map[string]any{
				"scan.requested": map[string]any{"topic": "stale"},
			},
		},
	})

	if resp.Error == nil || resp.Error.Code != -32602 {
		t.Fatalf("rpc error = %+v, want invalid params", resp.Error)
	}
	assertBuilderRootInputDiagnostic(t, resp.Error, "scan.requested", "not_declared_root_input", []string{"scan.corpus_file_requested"}, []string{"scan.corpus_file_requested"})
	if len(store.appended) != 0 {
		t.Fatalf("published events = %#v, want none before invalid input failure", store.appended)
	}
}

func TestHandlerRunStartRejectsDeclaredUnroutableInputBeforePublish(t *testing.T) {
	const eventName = "scan.unroutable_requested"
	bundle := runStartInputBundle(eventName)
	bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"] = runtimecontracts.SystemNodeContract{
		ID:           "scan-orchestrator",
		SubscribesTo: []string{"scan.other_requested"},
	}
	bundle.Nodes["scan-orchestrator"] = bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"]
	source := semanticview.Wrap(bundle)
	store := &runStartAppendStore{}
	_, acquirer := newTestOwnedEventBus(t, store, runtimebus.EventBusOptions{ContractBundle: source})
	handler := NewHandler(Options{
		AuthToken:       testBuilderAuthToken,
		SemanticSource:  source,
		RuntimeAcquirer: acquirer,
	})

	resp := callBuilderRPCRaw(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "reject-unroutable",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": "run-123",
			"inputs": map[string]any{
				eventName: map[string]any{"topic": "declared-but-unroutable"},
			},
		},
	})

	if resp.Error == nil || resp.Error.Code != -32602 {
		t.Fatalf("rpc error = %+v, want invalid params", resp.Error)
	}
	assertBuilderRootInputDiagnostic(t, resp.Error, eventName, "declared_root_input_not_routable", []string{eventName}, nil)
	if len(store.appended) != 0 {
		t.Fatalf("published events = %#v, want none before invalid input failure", store.appended)
	}
}

func assertBuilderRootInputDiagnostic(t *testing.T, rpcErr *RPCError, eventName, reason string, declared, routable []string) {
	t.Helper()
	data, ok := rpcErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("rpc error data = %T %#v, want structured map", rpcErr.Data, rpcErr.Data)
	}
	if data["event_name"] != eventName || data["reason"] != reason {
		t.Fatalf("rpc error data = %#v", data)
	}
	assertStringSlice := func(field string, want []string) {
		t.Helper()
		got, ok := data[field].([]string)
		if !ok {
			t.Fatalf("%s = %T %#v, want []string", field, data[field], data[field])
		}
		if len(got) != len(want) {
			t.Fatalf("%s = %#v, want %#v", field, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s = %#v, want %#v", field, got, want)
			}
		}
	}
	assertStringSlice("declared_events", declared)
	assertStringSlice("routable_events", routable)
	if rpcErr.Message == "" || rpcErr.Message == reason {
		t.Fatalf("rpc error message = %q, want author-facing summary", rpcErr.Message)
	}
}

func TestHandlerRunStartAcceptsDeclaredRoutableInput(t *testing.T) {
	const eventName = "scan.corpus_file_requested"
	runID := eventtest.UUID("builder-run-start-accepts-routable-input")
	source := semanticview.Wrap(runStartInputBundle(eventName))
	store := &runStartAppendStore{}
	_, acquirer := newTestOwnedEventBus(t, store, runtimebus.EventBusOptions{ContractBundle: source})
	handler := NewHandler(Options{
		AuthToken:       testBuilderAuthToken,
		SemanticSource:  source,
		RuntimeAcquirer: acquirer,
	})

	resp := callBuilderRPCRaw(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "accept",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": runID,
			"inputs": map[string]any{
				eventName: map[string]any{"request": map[string]any{"geography": "US"}},
			},
		},
	})

	if resp.Error != nil {
		t.Fatalf("unexpected rpc error %+v", resp.Error)
	}
	if len(store.appended) != 1 || store.appended[0] != eventName {
		t.Fatalf("published events = %#v, want %s", store.appended, eventName)
	}
}

func callBuilderRPCRaw(t *testing.T, httpHandler http.Handler, req Request) RPCResponse {
	t.Helper()
	h, ok := httpHandler.(*handler)
	if !ok {
		t.Fatalf("handler type = %T, want *handler", httpHandler)
	}
	result, rpcErr := h.dispatchRPC(context.Background(), req.Method, req.Params)
	return RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: result, Error: rpcErr}
}

func runStartInputBundle(eventName string) *runtimecontracts.WorkflowContractBundle {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Path:  "discovery",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{eventName},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": flow.Nodes["scan-orchestrator"],
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": &root.Children[0],
			},
		},
	}
}
