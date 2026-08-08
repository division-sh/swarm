package serveapp

import (
	"strings"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestAdmittedRuntimeModuleAndSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	module := &stubWorkflowModule{source: source}
	rt := &runtimepkg.Runtime{Options: runtimepkg.RuntimeOptions{WorkflowModule: module}}

	gotModule, gotSource, err := admittedRuntimeModuleAndSource(rt)
	if err != nil {
		t.Fatalf("admittedRuntimeModuleAndSource: %v", err)
	}
	if gotModule != module {
		t.Fatalf("module = %T %p, want %T %p", gotModule, gotModule, module, module)
	}
	if gotSource != source {
		t.Fatalf("source = %T, want exact admitted source %T", gotSource, source)
	}
}

func requireProviderTriggerEventSource(t *testing.T, source semanticview.Source, eventName string) triggergeneration.Generation {
	t.Helper()
	if source == nil {
		t.Fatal("semantic source is nil")
	}
	if _, ok := source.EventEntry(eventName); !ok {
		t.Fatalf("effective source %T does not expose imported event %q", source, eventName)
	}
	generation, _, ok := source.SemanticCapabilities().ProviderTriggerEvents()
	if !ok || !generation.Valid() {
		t.Fatalf("effective source %T has no valid provider-trigger generation", source)
	}
	return generation
}

func TestAdmittedRuntimeModuleAndSourceFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		runtime *runtimepkg.Runtime
		wantErr string
	}{
		{name: "nil runtime", wantErr: "admitted runtime is required"},
		{name: "nil module", runtime: &runtimepkg.Runtime{}, wantErr: "workflow module is required"},
		{
			name: "nil source",
			runtime: &runtimepkg.Runtime{Options: runtimepkg.RuntimeOptions{
				WorkflowModule: &stubWorkflowModule{},
			}},
			wantErr: "semantic source is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			module, source, err := admittedRuntimeModuleAndSource(test.runtime)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want %q", err, test.wantErr)
			}
			if module != nil || source != nil {
				t.Fatalf("partial result = module:%T source:%T, want none", module, source)
			}
		})
	}
}
