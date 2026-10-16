package runforkexecution

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type q1FreezeBetweenTransactions struct {
	SelectedContractForkLifecycle
	headers interface {
		LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	}
	materializations atomic.Int32
	children         [2]string
	secondReady      chan struct{}
	releaseSecond    chan struct{}
	refused          atomic.Int32
}

func (p *q1FreezeBetweenTransactions) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	index := int(p.materializations.Add(1)) - 1
	result, err := p.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		return result, err
	}
	if index >= len(p.children) {
		return result, fmt.Errorf("unexpected third materialization in two-child race")
	}
	source, err := p.headers.LoadRunHeader(ctx, req.SourceRunID)
	if err == nil && source.Status != "running" {
		err = fmt.Errorf("both children must materialize before the first freeze; source state=%s", source.Status)
	}
	if err != nil {
		return result, selectedMaterializationCommitTestError{value: result, cause: err}
	}
	p.children[index] = result.ForkRunID
	if index == 1 {
		close(p.secondReady)
	}
	return result, nil
}

func (p *q1FreezeBetweenTransactions) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	ready := p.secondReady
	if req.ForkRunID != p.children[0] {
		ready = p.releaseSecond
	}
	select {
	case <-ready:
		if req.ForkRunID == p.children[1] {
			if err := p.rejectCrossedAttachment(ctx, req); err != nil {
				return runfork.RunForkActivation{}, err
			}
		}
		return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
	case <-ctx.Done():
		return runfork.RunForkActivation{}, ctx.Err()
	}
}

func (p *q1FreezeBetweenTransactions) rejectCrossedAttachment(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) error {
	authority, found := effects.AuthorityFromContext(ctx)
	if !found || !authority.Valid() {
		return fmt.Errorf("second child has no valid attachment authority")
	}
	before, err := p.headers.LoadRunHeader(ctx, req.ForkRunID)
	if err != nil || before.Status != "paused" {
		return fmt.Errorf("second child must still be paused before attachment controls: %+v err=%v", before, err)
	}
	for _, fault := range []string{"other_child", "mismatched_fence"} {
		crossed := authority
		if fault == "other_child" {
			crossed.SelectedFork.ForkRunID = p.children[0]
		} else {
			crossed.FenceGeneration++
		}
		if !crossed.Valid() {
			return fmt.Errorf("%s control supplied structurally invalid authority", fault)
		}
		result, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(effects.WithAuthority(ctx, crossed), req)
		if err == nil || result.Activated {
			return fmt.Errorf("historical admission bypassed %s attachment: %+v %v", fault, result, err)
		}
		if fault == "other_child" {
			if err.Error() != "selected-contract activation requires exact attached execution authority" {
				return fmt.Errorf("wrong-child attachment hit another refusal: %w", err)
			}
		} else if failure, found := failures.EnvelopeFromError(err); !found || failure.Detail.Code != "external_effect_authority_stale" {
			return fmt.Errorf("mismatched-fence attachment hit another refusal: %w", err)
		}
		after, err := p.headers.LoadRunHeader(ctx, req.ForkRunID)
		if err != nil || !reflect.DeepEqual(before, after) {
			return fmt.Errorf("%s attachment changed the paused child: before=%+v after=%+v err=%v", fault, before, after, err)
		}
		p.refused.Add(1)
	}
	return nil
}

func proveSelectedQ1FrozenOwnerlessRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
			first, err := ExecuteSelectedContractRunFork(ctx, request)
			if err != nil || !first.Activation.Activated || !first.Activation.SourceFrozen {
				t.Fatalf("first child did not freeze the source: %+v %v", first, err)
			}
			selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
			headers := selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			frozen, err := headers.LoadRunHeader(ctx, request.SourceRunID)
			if err != nil || frozen.Status != "forked" || frozen.ContinuedAsRunID != first.Materialization.ForkRunID {
				t.Fatalf("source freeze precondition: %+v %v", frozen, err)
			}
			operation := *request.ForkOperation
			operation.OperationID, operation.IdempotencyKey = uuid.NewString(), uuid.NewString()
			request.ForkOperation = &operation
			materialized := abandonSelectedOperationPreparation(t, ctx, request)
			if materialized.ForkRunID == first.Materialization.ForkRunID {
				t.Fatal("independent historical preparation reused the first child")
			}
			retry := operation
			retry.OperationID = uuid.NewString()
			request.ForkOperation = &retry
			result, err := ExecuteSelectedContractRunFork(ctx, request)
			if err != nil || !result.Activation.Activated || !result.Activation.SourceFrozen || result.Activation.BranchDivergence != nil || result.Materialization.ForkRunID != materialized.ForkRunID {
				t.Fatalf("ownerless historical materialization was not recovered exactly: %+v %v", result, err)
			}
			record, found, err := request.Owner.ports.fork.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
			if err != nil || !found || record.Status != runfork.ForkOperationActivated || record.Request.OperationID != operation.OperationID || record.Result == nil || !record.Result.SourceFrozen || record.Result.SourceRunStatus != "forked" || record.ForkRunID != materialized.ForkRunID {
				t.Fatalf("historical recovery replaced its durable operation or immutable ACK: %+v found=%t err=%v", record, found, err)
			}
			after, err := headers.LoadRunHeader(ctx, request.SourceRunID)
			if err != nil || !reflect.DeepEqual(frozen, after) {
				t.Fatalf("historical recovery rewrote the original freeze: before=%+v after=%+v err=%v", frozen, after, err)
			}
			stored, err := storetest.ReadSelectedExecutionStorage(ctx, selected, materialized.ForkRunID)
			if err != nil || stored.Occurrences != 1 {
				t.Fatalf("historical recovery duplicated execution: %+v err=%v", stored, err)
			}
		})
	}
}

func proveSelectedQ1FirstFreezeRaceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			parent, request := ownerlessSelectedOperationFixture(t, backend)
			ctx, cancel := context.WithCancel(parent)
			selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
			headers := selected.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
			})
			probe := &q1FreezeBetweenTransactions{SelectedContractForkLifecycle: request.Owner.ports.fork, headers: headers,
				secondReady: make(chan struct{}), releaseSecond: make(chan struct{})}
			request.Owner.ports.fork = probe
			type outcome struct {
				result SelectedContractExecutionResult
				err    error
			}
			results, joined := make(chan outcome, 2), make(chan struct{}, 2)
			var release sync.Once
			defer func() {
				cancel()
				release.Do(func() { close(probe.releaseSecond) })
				for range 2 {
					<-joined
				}
			}()
			for range 2 {
				invocation := request
				operation := *request.ForkOperation
				operation.OperationID, operation.IdempotencyKey = uuid.NewString(), uuid.NewString()
				invocation.ForkOperation = &operation
				go func() {
					defer func() { joined <- struct{}{} }()
					result, err := ExecuteSelectedContractRunFork(ctx, invocation)
					results <- outcome{result, err}
				}()
			}
			await := func() outcome {
				t.Helper()
				select {
				case result := <-results:
					return result
				case <-ctx.Done():
					t.Fatal(ctx.Err())
					return outcome{}
				}
			}
			first := await()
			if first.err != nil || !first.result.Activation.Activated || !first.result.Activation.SourceFrozen || first.result.Materialization.ForkRunID != probe.children[0] {
				t.Fatalf("first child failed to perform the original freeze: %+v %v", first.result, first.err)
			}
			frozen, err := headers.LoadRunHeader(ctx, request.SourceRunID)
			if err != nil || frozen.Status != "forked" || frozen.ContinuedAsRunID != probe.children[0] || frozen.EndedAt == nil {
				t.Fatalf("original source freeze lineage: %+v %v", frozen, err)
			}
			paused, err := headers.LoadRunHeader(ctx, probe.children[1])
			if err != nil || paused.Status != "paused" {
				t.Fatalf("second child bypassed the activation barrier: %+v %v", paused, err)
			}
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, request.SourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			release.Do(func() { close(probe.releaseSecond) })
			second := await()
			if second.err != nil || !second.result.Activation.Activated || !second.result.Activation.SourceFrozen || second.result.Activation.SourceRunStatus != "forked" ||
				second.result.Activation.BranchDivergence != nil || second.result.Materialization.ForkRunID != probe.children[1] || probe.children[0] == probe.children[1] ||
				!first.result.Materialization.ForkPoint.SameIdentity(second.result.Materialization.ForkPoint) || probe.materializations.Load() != 2 || probe.refused.Load() != 2 {
				t.Fatalf("second activation did not re-prove historical source admission: %+v %v", second.result, second.err)
			}
			afterHeader, err := headers.LoadRunHeader(ctx, request.SourceRunID)
			if err != nil || !reflect.DeepEqual(frozen, afterHeader) {
				t.Fatalf("second activation rewrote the original source freeze: before=%+v after=%+v err=%v", frozen, afterHeader, err)
			}
			after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, request.SourceRunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("second activation changed frozen source domain: %v", err)
			}
			for _, child := range probe.children {
				stored, err := storetest.ReadSelectedExecutionStorage(ctx, selected, child)
				if err != nil || stored.Occurrences != 1 || stored.RunStatus == "failed" || stored.RunStatus == "cancelled" {
					t.Fatalf("independent current child authority: child=%s stored=%+v err=%v", child, stored, err)
				}
			}
			if err := request.Owner.RetireSelectedContexts(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
