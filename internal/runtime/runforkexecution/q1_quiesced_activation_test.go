package runforkexecution

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type q1QuiescedActivationProbe struct {
	SelectedContractForkLifecycle
	quiesce func(context.Context, effects.Authority) error
	observe func(context.Context) (map[string]storetest.SelectedForkStorageTableSnapshot, error)
	refused bool
}

func (p *q1QuiescedActivationProbe) ActivateRunForkForSelectedContractExecution(ctx context.Context, request runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	authority, found := effects.AuthorityFromContext(ctx)
	if !found || !authority.Valid() || authority.SelectedFork.ForkRunID != request.ForkRunID {
		return runfork.RunForkActivation{}, fmt.Errorf("control did not reach the exact claimed child attachment")
	}
	if err := p.quiesce(ctx, authority); err != nil {
		return runfork.RunForkActivation{}, fmt.Errorf("quiesce the original native execution: %w", err)
	}
	before, err := p.observe(ctx)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	activation, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, request)
	if err == nil || activation.Activated || err.Error() != "selected-contract activation requires the current running prepared executor" {
		return activation, fmt.Errorf("quiesced historical child missed its exact attachment refusal: %+v err=%v", activation, err)
	}
	after, readErr := p.observe(ctx)
	if readErr != nil || !reflect.DeepEqual(before, after) {
		return activation, fmt.Errorf("quiesced attachment changed durable application evidence: %v", readErr)
	}
	p.refused = true
	return activation, err
}

func proveSelectedQ1QuiescedHistoricalAttachmentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
			first, err := ExecuteSelectedContractRunFork(ctx, request)
			if err != nil || !first.Activation.Activated || !first.Activation.SourceFrozen {
				t.Fatalf("first child did not establish canonical freeze: %+v err=%v", first, err)
			}
			selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
			if observed, err := observeSelectedRetainedExecution(t, ctx, request.Owner, selected, first.Materialization.ForkRunID); err != nil || observed.ExecutedEventCount != 1 {
				t.Fatalf("first child did not settle before isolated attachment proof: %+v err=%v", observed, err)
			}
			headers := selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			frozen, err := headers.LoadRunHeader(ctx, request.SourceRunID)
			if err != nil || frozen.Status != "forked" || frozen.ContinuedAsRunID != first.Materialization.ForkRunID {
				t.Fatalf("source freeze precondition: %+v err=%v", frozen, err)
			}
			probe := &q1QuiescedActivationProbe{SelectedContractForkLifecycle: request.Owner.ports.fork,
				quiesce: request.Owner.ports.runtimeExecution.QuiesceRunForkSelectedContractRuntimeExecution,
				observe: func(ctx context.Context) (map[string]storetest.SelectedForkStorageTableSnapshot, error) {
					return storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				}}
			request.Owner.ports.fork = probe
			operation := *request.ForkOperation
			operation.OperationID, operation.IdempotencyKey = uuid.NewString(), uuid.NewString()
			request.ForkOperation = &operation
			result, err := ExecuteSelectedContractRunFork(ctx, request)
			if err == nil || !strings.Contains(err.Error(), "selected-contract activation requires the current running prepared executor") || result.Activation.Activated || !probe.refused {
				t.Fatalf("historical admission revived the natively quiesced executor: %+v refused=%t err=%v", result, probe.refused, err)
			}
			after, err := headers.LoadRunHeader(ctx, request.SourceRunID)
			if err != nil || !reflect.DeepEqual(frozen, after) {
				t.Fatalf("refused historical child rewrote the source freeze: before=%+v after=%+v err=%v", frozen, after, err)
			}
		})
	}
}
