package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestFlowActivationRequestFreezesExactPlanAndResolutionAuthority(t *testing.T) {
	binding := processbinding.Binding{ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "request-test", ProcessBootID: uuid.NewString(), GenerationGrantID: uuid.NewString(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1}
	plan := DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString(), WorkflowVersion: "1", BundleHash: binding.BundleHash, ExecutionMode: executionmode.Mock,
		Identity: flowidentity.Instance{TemplateID: "child", ScopeKey: "child", InstanceID: "one", InstancePath: "child/one", EntityID: uuid.NewString(), HasStoredPath: true}}
	plan.CreationEvent = &DynamicFlowRuntimeCreationEventPlan{EventID: uuid.NewString(), EventType: "child.created", RunID: plan.RunID, ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Mock, Payload: []byte(`{"value":7.0}`), CreatedAt: time.Now().UTC()}
	request := NewDynamicFlowRuntimeActivationRequest(plan, 3, "aborted", binding)
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	hash, err := request.Plan().Hash()
	if err != nil {
		t.Fatal(err)
	}
	plan.WorkflowVersion = "changed"
	plan.CreationEvent.Payload[9] = '9'
	returned := request.Plan()
	returned.CreationEvent.Payload[9] = '8'
	if got, err := request.Plan().Hash(); err != nil || got != hash {
		t.Fatalf("mutable plan changed retained authority: %s %v", got, err)
	}
	if request.Predecessor() != 3 || request.PredecessorDisposition() != "aborted" {
		t.Fatal("request lost observed predecessor")
	}
	for _, disposition := range []string{"", "retiring", "ready"} {
		if err := NewDynamicFlowRuntimeActivationRequest(request.Plan(), 3, disposition, binding).Validate(); err == nil {
			t.Fatalf("invalid observed disposition %q admitted", disposition)
		}
	}
	attempt, err := NewDynamicFlowRuntimeActivationAttempt("4", request.Plan().RunID, request.Plan().Identity.InstancePath, binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []FlowActivationResolution{FlowActivationUnresolved, FlowActivationUnadmitted, FlowActivationForeign, FlowActivationResolution(99)} {
		if request.ValidateResolution(DynamicFlowRuntimeActivationResolution{Disposition: kind, Attempt: attempt}) == nil {
			t.Fatalf("non-admitted resolution %d leaked executable authority", kind)
		}
	}
	if err := request.ValidateResolution(DynamicFlowRuntimeActivationResolution{Disposition: FlowActivationAdmitted, Attempt: attempt}); err != nil {
		t.Fatal(err)
	}
	foreign, err := NewDynamicFlowRuntimeActivationAttempt("4", uuid.NewString(), attempt.InstancePath(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if request.ValidateResolution(DynamicFlowRuntimeActivationResolution{Disposition: FlowActivationAdmitted, Attempt: foreign}) == nil {
		t.Fatal("foreign resolution acquired cleanup authority")
	}
}
