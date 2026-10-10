package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestSelectedForkActivationAcknowledgementControlsResources(t *testing.T) {
	cleanup := errors.New("activation postcommit cleanup")
	verifyErr := errors.New("independent activation verification failure")
	for _, tc := range []struct {
		name                string
		ack                 bool
		beginErr, verifyErr error
		wantVerify          int
	}{
		{name: "acknowledged_cleanup", ack: true, beginErr: cleanup, wantVerify: 1},
		{name: "acknowledged_then_verify_failure", ack: true, beginErr: cleanup, verifyErr: verifyErr, wantVerify: 1},
		{name: "missing_ack_with_error", beginErr: cleanup},
		{name: "missing_ack_without_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := processbinding.Binding{
				ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "selected-activation-test", ProcessBootID: uuid.NewString(),
				GenerationGrantID: uuid.NewString(), BundleHash: runForkTestBundleHash,
				RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
			}
			plan := pipeline.DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString(), BundleHash: binding.BundleHash, WorkflowVersion: "1", ExecutionMode: executionmode.Mock,
				Identity: flowidentity.Instance{TemplateID: "review", ScopeKey: "review", InstanceID: "one", InstancePath: "review/one", EntityID: uuid.NewString(), HasStoredPath: true}}
			identity, err := flowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := pipeline.NewDynamicFlowRuntimeActivationAttempt("1", identity.RunID, identity.Route.InstancePath, binding)
			if err != nil {
				t.Fatal(err)
			}
			var inventory []selectedFlowActivation
			probe := &selectedPendingAdmissionProbe{inventory: &inventory, acknowledged: tc.ack, beginErr: tc.beginErr, verifyErr: tc.verifyErr,
				readiness: pipeline.DynamicFlowRuntimeReadiness{Plan: plan, AttemptOrdinal: 1, AttemptState: "planned", RunStatus: "running", InstanceStatus: "active", Phase: pipeline.FlowAttachmentPlanned}}
			if tc.ack {
				probe.resolved = pipeline.DynamicFlowRuntimeActivationResolution{Disposition: pipeline.FlowActivationAdmitted, Attempt: attempt}
			}
			diagnostics := &selectedForkCommitDiagnostics{}
			err = admitSelectedContractFlowActivation(context.Background(), probe, binding, identity, diagnostics, &inventory)
			if len(inventory) != 1 || inventory[0].identity != identity || probe.verifications != tc.wantVerify || probe.timerRetirements != 0 || probe.abandonments != 0 {
				t.Fatalf("admission acquired wrong resources: inventory=%+v probe=%+v", inventory, probe)
			}
			if tc.ack {
				if inventory[0].attempt != attempt || inventory[0].pending != nil || !errors.Is(diagnostics.err(), cleanup) || !errors.Is(err, tc.verifyErr) {
					t.Fatalf("acknowledged activation err=%v diagnostic=%v", err, diagnostics.err())
				}
			} else if err == nil || inventory[0].pending == nil || diagnostics.err() != nil {
				t.Fatalf("unacknowledged activation err=%v inventory=%+v diagnostic=%v", err, inventory, diagnostics.err())
			}
		})
	}
}
