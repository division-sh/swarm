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

type selectedPendingAdmissionProbe struct {
	readiness                                     pipeline.DynamicFlowRuntimeReadiness
	inventory                                     *[]selectedFlowActivation
	resolved                                      pipeline.DynamicFlowRuntimeActivationResolution
	resolveErr                                    error
	panicAfterCommit                              bool
	acknowledged                                  bool
	beginErr, verifyErr                           error
	verifications, abandonments, timerRetirements int
	request                                       pipeline.DynamicFlowRuntimeActivationRequest
}

func (p *selectedPendingAdmissionProbe) LoadDynamicFlowRuntimeReadiness(context.Context, string, flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	return p.readiness, true, nil
}

func (p *selectedPendingAdmissionProbe) BeginDynamicFlowRuntimeActivation(_ context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	if len(*p.inventory) != 1 || (*p.inventory)[0].pending == nil || (*p.inventory)[0].pending.ID() != request.ID() {
		return pipeline.DynamicFlowRuntimeActivationAdmissionResult{}, errors.New("selected Begin has no retained admission responsibility")
	}
	p.request = request
	if p.panicAfterCommit {
		panic("selected Begin lost its response")
	}
	if p.acknowledged {
		return pipeline.DynamicFlowRuntimeActivationAdmissionResult{Acknowledged: true, Attempt: p.resolved.Attempt}, p.beginErr
	}
	return pipeline.DynamicFlowRuntimeActivationAdmissionResult{}, p.beginErr
}

func (p *selectedPendingAdmissionProbe) ResolveDynamicFlowRuntimeActivation(_ context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationResolution, error) {
	if request.ID() != p.request.ID() {
		return pipeline.DynamicFlowRuntimeActivationResolution{}, errors.New("resolution replaced exact pending request")
	}
	return p.resolved, p.resolveErr
}

func (p *selectedPendingAdmissionProbe) VerifyDynamicFlowRuntimeActivationAttempt(context.Context, pipeline.DynamicFlowRuntimeActivationAttempt) error {
	p.verifications++
	return p.verifyErr
}

func (p *selectedPendingAdmissionProbe) AbandonDynamicFlowRuntimeActivationAttempt(_ context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	if attempt != p.resolved.Attempt {
		return errors.New("abandonment changed exact resolved authority")
	}
	p.abandonments++
	return nil
}

func (p *selectedPendingAdmissionProbe) RetireInitialEntryTimerWakeups(context.Context, flowidentity.RunScopedFlowInstance) error {
	p.timerRetirements++
	return nil
}

func TestSelectedAdmissionResponsibilitySurvivesLostResponseAndPanic(t *testing.T) {
	for _, cut := range []string{"committed", "unadmitted", "foreign", "unresolved", "panic", "wrong_receipt", "invalid_disposition"} {
		t.Run(cut, func(t *testing.T) {
			binding := processbinding.Binding{ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "selected-admission", ProcessBootID: uuid.NewString(), GenerationGrantID: uuid.NewString(), BundleHash: runForkTestBundleHash, RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1}
			plan := pipeline.DynamicFlowRuntimeReadinessPlan{RunID: uuid.NewString(), BundleHash: binding.BundleHash, WorkflowVersion: "1", ExecutionMode: executionmode.Mock,
				Identity: flowidentity.Instance{TemplateID: "review", ScopeKey: "review", InstanceID: "one", InstancePath: "review/one", EntityID: uuid.NewString(), HasStoredPath: true}}
			identity, err := flowidentity.NewRunScopedFlowInstance(plan.RunID, plan.Identity.Route())
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := pipeline.NewDynamicFlowRuntimeActivationAttempt("1", plan.RunID, plan.Identity.InstancePath, binding)
			if err != nil {
				t.Fatal(err)
			}
			var inventory []selectedFlowActivation
			probe := &selectedPendingAdmissionProbe{inventory: &inventory, readiness: pipeline.DynamicFlowRuntimeReadiness{Plan: plan, AttemptOrdinal: 1, AttemptState: "planned", RunStatus: "running", InstanceStatus: "active", Phase: pipeline.FlowAttachmentPlanned}, panicAfterCommit: cut == "panic", beginErr: errors.New("selected Begin lost its response")}
			probe.resolved = pipeline.DynamicFlowRuntimeActivationResolution{Disposition: pipeline.FlowActivationAdmitted, Attempt: attempt}
			switch cut {
			case "unadmitted":
				probe.resolved = pipeline.DynamicFlowRuntimeActivationResolution{Disposition: pipeline.FlowActivationUnadmitted}
			case "foreign":
				probe.resolved = pipeline.DynamicFlowRuntimeActivationResolution{Disposition: pipeline.FlowActivationForeign}
			case "unresolved", "panic":
				probe.resolved = pipeline.DynamicFlowRuntimeActivationResolution{}
				probe.resolveErr = errors.New("persistent resolution failure")
			case "wrong_receipt":
				probe.resolved.Attempt, _ = pipeline.NewDynamicFlowRuntimeActivationAttempt("1", uuid.NewString(), plan.Identity.InstancePath, binding)
			case "invalid_disposition":
				probe.resolved.Disposition = pipeline.FlowActivationResolution(99)
			}
			diagnostics := &selectedForkCommitDiagnostics{}
			var admissionErr error
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				admissionErr = admitSelectedContractFlowActivation(context.Background(), probe, binding, identity, diagnostics, &inventory)
			}()
			if len(inventory) != 1 || probe.request.ID() == "" || probe.timerRetirements != 0 {
				t.Fatalf("lost exact non-executable inventory: %+v", inventory)
			}
			if cut == "committed" {
				if admissionErr != nil || diagnostics.err() == nil || inventory[0].pending != nil || probe.verifications == 0 {
					t.Fatalf("committed admission lost receipt/diagnostic: %v %+v", admissionErr, inventory)
				}
			} else if admissionErr == nil && panicked == nil {
				t.Fatal("unresolved/foreign admission executed")
			}
			if cut == "panic" && panicked == nil {
				t.Fatal("panic was hidden")
			}
			if cut != "committed" {
				retained, cleanupErr := retireSelectedFlowActivations(context.Background(), probe, inventory)
				if cut == "foreign" || cut == "unadmitted" {
					if cleanupErr != nil || len(retained) != 0 || probe.abandonments != 0 {
						t.Fatal("non-owned resolution acquired cleanup authority")
					}
					return
				}
				if cleanupErr == nil || len(retained) != 1 || retained[0].pending == nil || probe.abandonments != 0 {
					t.Fatal("uncertain admission lost its retained owner")
				}
				probe.resolveErr = nil
				probe.resolved = pipeline.DynamicFlowRuntimeActivationResolution{Disposition: pipeline.FlowActivationAdmitted, Attempt: attempt}
				retained, cleanupErr = retireSelectedFlowActivations(context.Background(), probe, retained)
				if cleanupErr != nil || len(retained) != 0 || probe.abandonments != 1 || probe.timerRetirements != 0 {
					t.Fatalf("pending cleanup acquired resources: retained=%+v err=%v", retained, cleanupErr)
				}
			}
		})
	}
}
