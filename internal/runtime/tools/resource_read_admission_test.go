package tools

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
)

func TestResourceReadAdmissionPrecedence(t *testing.T) {
	source, _ := loadResourceDataToolSource(t)
	actor := flowDataActorWithIdentity(t, source, "admission")
	ref := flowdata.AllowedResourceData(source, actor)[0]
	for _, tc := range []struct {
		name, want string
		ctx        context.Context
		ref        *durabledata.DeclarationRef
		store      bool
	}{
		{"run_before_declaration", "requires run context", models.WithActor(unmanagedToolTestContext(), actor), nil, true},
		{"declaration_before_store", "requires one structured declaration", flowDataToolContext(actor), nil, false},
		{"store_after_authorization", "selected-store reader is required", flowDataToolContext(actor), &ref, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &resourceReadRecordingStore{}
			opts := ExecutorOptions{WorkflowSource: source}
			if tc.store {
				opts.DataAccessStore = store
			}
			executor := NewExecutorWithOptions(nil, opts)
			result, err := executor.execReadFlowData(tc.ctx, actor, map[string]any{"kind": "resource_row", "declaration": tc.ref})
			if result != nil || err == nil || !strings.Contains(err.Error(), tc.want) || len(store.runs) != 0 {
				t.Fatalf("result=%+v err=%v reads=%+v", result, err, store.runs)
			}
		})
	}
	for _, foreign := range []durabledata.DeclarationRef{
		{FlowPath: "other", EventName: ref.EventName},
		{FlowPath: ref.FlowPath, EventName: "support/other.loaded"},
	} {
		store := &resourceReadRecordingStore{}
		executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source, DataAccessStore: store})
		if result, err := executor.Execute(flowDataToolContext(actor), "read_flow_data", map[string]any{"kind": "resource_row", "declaration": foreign, "key": "a"}); result != nil || err == nil || len(store.runs) != 0 {
			t.Fatalf("foreign pair %+v: result=%+v err=%v reads=%+v", foreign, result, err, store.runs)
		}
	}
}
