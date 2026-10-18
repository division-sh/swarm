package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type preparedForkCustodyProbe struct {
	t        *testing.T
	process  startupownership.Authority
	calls    int
	retained context.Context
	failure  error
}

func (p *preparedForkCustodyProbe) enter(ctx context.Context, kind startupownership.PreparedOperationKind) error {
	p.t.Helper()
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil || original.HasRuntimeGrant() || original.ProcessAuthority() != p.process || !startupownership.PreparedOperationEntered(ctx, kind, p) {
		p.t.Fatalf("typed selected command lost its exact original: %v", err)
	}
	p.calls++
	p.retained = context.WithoutCancel(ctx)
	return p.failure
}

func (p *preparedForkCustodyProbe) LoadRunForkSelectedContractSourceEvents(ctx context.Context, _, _ string, _ []string, _ semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	return []runfork.RunForkSelectedContractSourceEvent{{}}, p.enter(ctx, startupownership.PreparedForkSourceEvents)
}
func (p *preparedForkCustodyProbe) HeartbeatRunForkSelectedContractRuntimeExecution(ctx context.Context, _ effects.Authority, _ time.Duration) error {
	return p.enter(ctx, startupownership.PreparedForkSettlement)
}
func (p *preparedForkCustodyProbe) QuiesceRunForkSelectedContractRuntimeExecution(ctx context.Context, _ effects.Authority) error {
	return p.enter(ctx, startupownership.PreparedForkSettlement)
}
func (p *preparedForkCustodyProbe) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, _ effects.Authority, _ json.RawMessage) error {
	return p.enter(ctx, startupownership.PreparedForkSettlement)
}
func (p *preparedForkCustodyProbe) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, _ string) error {
	return p.enter(ctx, startupownership.PreparedForkSettlement)
}
func (p *preparedForkCustodyProbe) FailMaterializedSelectedContractExecutionFork(ctx context.Context, _ string, _ runfork.ForkOperationFailure) error {
	return p.enter(ctx, startupownership.PreparedForkConstructionFailure)
}
func (p *preparedForkCustodyProbe) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, _ string) error {
	return p.enter(ctx, startupownership.PreparedForkConstructionFailure)
}
func (p *preparedForkCustodyProbe) ReconcileSelectedSuccessorDeliveryAuthority(ctx context.Context, _ string, _ deliverylifecycle.ExecutionAuthority) error {
	return p.enter(ctx, startupownership.PreparedForkDeliveryReconcile)
}
func (p *preparedForkCustodyProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, _ runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	return runfork.RunForkActivation{Activated: true}, p.enter(ctx, startupownership.PreparedForkActivate)
}

// These component controls use the same real native process as the surface
// matrix. Full selected journeys separately prove the native semantic writers.
func preparedForkCustodyControls(t *testing.T, process startupownership.ProcessCapability, original startupownership.Authority, coordinates managedcapabilities.SelectedForkPreparationCoordinates) func(context.Context, bool) {
	t.Helper()
	binding := runfork.SelectedForkPreparationBinding{ForkRunID: uuid.NewString(), SelectedForkPreparation: runfork.SelectedForkPreparation{
		PreparationID: uuid.NewString(), ProcessGeneration: original.AuthorityGeneration, Coordinates: coordinates,
		DeclarationPlanFingerprint: "sha256:" + strings.Repeat("1", 64), SourceRunID: uuid.NewString(), Actors: []runfork.SelectedForkPreparedActor{},
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: uuid.NewString()},
	}}
	binding.ForkEventID = binding.ForkPoint.EventID
	authority := effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: uuid.NewString(), ExecutionOwner: original.OwnerID,
		FenceGeneration: original.AuthorityGeneration, LeaseExpiresAt: time.Now().Add(time.Minute), ExecutionMode: effects.ExecutionModeLive,
		SelectedFork: effects.SelectedContractForkAuthority{ForkRunID: binding.ForkRunID, Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config"}}
	authority.SelectedFork.ExecutionID = authority.ID
	source, err := correlation.NewSourceArtifactFact(coordinates.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	successor, err := deliverylifecycle.NewSelectedExecutionAuthority(source, authority.ID, binding.ForkRunID, 1)
	if err != nil {
		t.Fatal(err)
	}
	probe := &preparedForkCustodyProbe{t: t, process: original}
	var events startupownership.PreparedForkSourceEventsResult
	var activation runfork.RunForkActivation
	constructors := map[string]func(context.Context) (startupownership.PreparedProcessOperation, error){
		"source": func(ctx context.Context) (startupownership.PreparedProcessOperation, error) {
			return startupownership.PreparedForkSourceEventsOperation(ctx, process, probe, binding, authority, []string{binding.ForkEventID}, semanticview.OriginalLoopCarriage{}, &events)
		},
		"activation": func(ctx context.Context) (startupownership.PreparedProcessOperation, error) {
			return startupownership.PreparedForkActivateOperation(ctx, process, probe, binding, authority, runfork.RunForkSelectedContractExecutionActivateRequest{ForkRunID: binding.ForkRunID}, &activation)
		},
		"reconcile": func(ctx context.Context) (startupownership.PreparedProcessOperation, error) {
			return startupownership.PreparedForkDeliveryReconcileOperation(ctx, process, probe, binding, authority, binding.SourceRunID, successor)
		},
	}
	for _, discard := range []bool{false, true} {
		name := "construction_fail"
		if discard {
			name = "construction_discard"
		}
		constructors[name] = func(ctx context.Context) (startupownership.PreparedProcessOperation, error) {
			return startupownership.PreparedForkConstructionFailureOperation(ctx, process, probe, binding, runfork.ForkOperationFailure{}, discard)
		}
	}
	for _, kind := range []startupownership.PreparedForkSettlementKind{startupownership.PreparedForkHeartbeat, startupownership.PreparedForkQuiesce, startupownership.PreparedForkFail, startupownership.PreparedForkClose} {
		constructors[string(rune('0'+kind))] = func(ctx context.Context) (startupownership.PreparedProcessOperation, error) {
			return startupownership.PreparedForkSettlementOperation(ctx, process, probe, binding, authority, kind, json.RawMessage(`{"reason":"exact"}`), time.Minute)
		}
	}
	return func(ctx context.Context, admitted bool) {
		t.Helper()
		for name, construct := range constructors {
			before := probe.calls
			operation, err := construct(ctx)
			if err == nil {
				err = process.RunPreparedOperation(ctx, operation)
			}
			if !admitted {
				if err == nil || probe.calls != before {
					t.Fatalf("refused %s reached its semantic owner: calls=%d err=%v", name, probe.calls-before, err)
				}
				continue
			}
			if err != nil || probe.calls != before+1 {
				t.Fatalf("exact %s custody failed: calls=%d err=%v", name, probe.calls-before, err)
			}
			if name == "source" && !reflect.DeepEqual(events.Events, []runfork.RunForkSelectedContractSourceEvent{{}}) || name == "activation" && !activation.Activated {
				t.Fatalf("exact %s result changed", name)
			}
			probe.failure = errors.New("exact native committed-result error")
			operation, err = construct(ctx)
			if err == nil {
				err = process.RunPreparedOperation(ctx, operation)
			}
			if !errors.Is(err, probe.failure) || probe.calls != before+2 {
				t.Fatalf("%s changed semantic error/result: %v", name, err)
			}
			if name == "source" {
				_, native := events.Outcome(err)
				if native != probe.failure {
					t.Fatal("source command erased its exact semantic result")
				}
				joined := errors.Join(err, context.Canceled)
				if _, preserved := events.Outcome(joined); preserved != joined {
					t.Fatal("source result hid independent cancellation or possession failure")
				}
			}
			probe.failure = nil
			if startupownership.PreparedOperationEntered(probe.retained, startupownership.PreparedForkSourceEvents, probe) {
				t.Fatal("completed preparation scope escaped its synchronous command")
			}
		}
		if admitted {
			saved := binding
			for _, field := range []string{"process", "owner", "boot", "generation", "fork"} {
				binding = saved
				switch field {
				case "process":
					binding.Coordinates.ProcessAuthorityID = uuid.NewString()
				case "owner":
					binding.Coordinates.ProcessOwnerID = "foreign-owner"
				case "boot":
					binding.Coordinates.ProcessBootID = uuid.NewString()
				case "generation":
					binding.ProcessGeneration++
				case "fork":
					binding.ForkRunID = uuid.NewString()
				}
				for name, construct := range constructors {
					if field == "fork" && (name == "construction_fail" || name == "construction_discard") {
						continue // The native construction owner validates these run-only commands.
					}
					before := probe.calls
					operation, err := construct(ctx)
					if err == nil {
						err = process.RunPreparedOperation(ctx, operation)
					}
					if err == nil || probe.calls != before {
						t.Fatalf("crossed %s/%s reached its semantic owner: %v", name, field, err)
					}
				}
			}
			binding = saved
		}
	}
}
