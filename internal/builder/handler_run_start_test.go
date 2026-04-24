package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/flowmodel"
	"swarm/internal/runtime/semanticview"
)

type runStartAppendStore struct {
	appended []string
}

func (s *runStartAppendStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.appended = append(s.appended, string(evt.Type))
	return nil
}

func (*runStartAppendStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (*runStartAppendStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func TestHandlerRunStartRejectsUndeclaredInputBeforePublish(t *testing.T) {
	source := semanticview.Wrap(runStartInputBundle("scan.corpus_file_requested"))
	store := &runStartAppendStore{}
	bus, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := NewHandler(Options{
		AuthToken:      testBuilderAuthToken,
		SemanticSource: source,
		CurrentRuntime: func() *runtimepkg.Runtime {
			return &runtimepkg.Runtime{Bus: bus}
		},
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
	if len(store.appended) != 0 {
		t.Fatalf("published events = %#v, want none before invalid input failure", store.appended)
	}
}

func TestHandlerRunStartAcceptsDeclaredRoutableInput(t *testing.T) {
	const eventName = "scan.corpus_file_requested"
	source := semanticview.Wrap(runStartInputBundle(eventName))
	store := &runStartAppendStore{}
	bus, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	handler := NewHandler(Options{
		AuthToken:      testBuilderAuthToken,
		SemanticSource: source,
		CurrentRuntime: func() *runtimepkg.Runtime {
			return &runtimepkg.Runtime{Bus: bus}
		},
	})

	resp := callBuilderRPCRaw(t, handler, Request{
		JSONRPC: "2.0",
		ID:      "accept",
		Method:  "run.start",
		Params: map[string]any{
			"run_id": "run-123",
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

func callBuilderRPCRaw(t *testing.T, handler http.Handler, req Request) RPCResponse {
	t.Helper()
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	rec := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(raw))
	httpReq.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
	handler.ServeHTTP(rec, httpReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d body=%s", rec.Code, rec.Body.String())
	}
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
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
