package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedOperationContinuationKeepsIndependentErrors(t *testing.T) {
	replay := &runfork.ForkOperationReplayRequiredError{OperationID: uuid.NewString(), ForkRunID: uuid.NewString()}
	for _, err := range []error{replay, fmt.Errorf("native disposition: %w", replay), errors.Join(replay)} {
		if isolatedSelectedForkOperationReplay(err) != replay {
			t.Fatalf("isolated election was not retained: %v", err)
		}
	}
	for _, err := range []error{nil, context.Canceled, errors.Join(replay, context.Canceled), errors.Join(replay, errors.New("independent cleanup")),
		errors.Join(replay, &runfork.ForkOperationReplayRequiredError{OperationID: replay.OperationID, ForkRunID: replay.ForkRunID})} {
		if isolatedSelectedForkOperationReplay(err) != nil {
			t.Fatalf("joined failure became election authority: %v", err)
		}
	}
}

type operationRecoveryProbe struct {
	SelectedContractForkLifecycle
	reads      atomic.Int32
	recoveries atomic.Int32
	readSecond chan struct{}
	entered    chan struct{}
	allow      chan struct{}
	cause      error
	uncertain  bool
	readOnce   sync.Once
}

func (p *operationRecoveryProbe) LoadForkOperation(ctx context.Context, actor, key, hash string) (runfork.ForkOperationRecord, bool, error) {
	record, found, err := p.SelectedContractForkLifecycle.LoadForkOperation(ctx, actor, key, hash)
	if p.reads.Add(1) >= 2 && p.readSecond != nil {
		p.readOnce.Do(func() { close(p.readSecond) })
	}
	if found && err == nil && p.uncertain {
		record.Status = runfork.ForkOperationUncertain
		record.Failure = &runfork.ForkOperationFailure{Code: "provider_uncertain", Reason: "external outcome remains uncertain"}
	}
	return record, found, err
}

func (p *operationRecoveryProbe) RecoverSelectedFork(ctx context.Context, request runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error) {
	count := p.recoveries.Add(1)
	if p.cause != nil {
		return runfork.SelectedForkRecoveryResult{}, p.cause
	}
	if p.entered != nil {
		if count == 1 {
			close(p.entered)
		}
		select {
		case <-p.allow:
		case <-ctx.Done():
			return runfork.SelectedForkRecoveryResult{}, ctx.Err()
		}
	}
	return p.SelectedContractForkLifecycle.RecoverSelectedFork(ctx, request)
}

func TestSelectedForkOwnerlessObserversShareRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, request := ownerlessSelectedOperationFixture(t, backend)
			ctx, cancel := context.WithCancel(ctx)
			materialized := abandonSelectedOperationPreparation(t, ctx, request)
			probe := &operationRecoveryProbe{SelectedContractForkLifecycle: request.Owner.ports.fork,
				readSecond: make(chan struct{}), entered: make(chan struct{}), allow: make(chan struct{})}
			request.Owner.ports.fork = probe
			type outcome struct {
				result SelectedContractExecutionResult
				err    error
			}
			results := make(chan outcome, 2)
			var release sync.Once
			var workers sync.WaitGroup
			defer func() {
				release.Do(func() { close(probe.allow) })
				cancel()
				workers.Wait()
			}()
			start := func() {
				duplicate := request
				operation := *request.ForkOperation
				operation.OperationID = uuid.NewString()
				duplicate.ForkOperation = &operation
				workers.Add(1)
				go func() {
					defer workers.Done()
					result, err := ExecuteSelectedContractRunFork(ctx, duplicate)
					results <- outcome{result, err}
				}()
			}
			start()
			select {
			case <-probe.entered:
			case result := <-results:
				t.Fatalf("first observer did not enter exact native recovery: %+v/%v", result.result, result.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			start()
			select {
			case <-probe.readSecond:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			release.Do(func() { close(probe.allow) })
			for range 2 {
				result := <-results
				if result.err != nil || !result.result.Activation.Activated || result.result.Materialization.ForkRunID != materialized.ForkRunID {
					t.Errorf("recovery observer changed the committed child: %+v/%v", result.result, result.err)
				}
			}
			if probe.recoveries.Load() != 1 {
				t.Fatalf("ownerless observers attached competing native recoveries: %d", probe.recoveries.Load())
			}
		})
	}
}

func TestSelectedOperationRefusalsDoNotCreateAnotherChildBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, refusal := range []string{"failed", "uncertain_observation", "independent_recovery_failure"} {
			t.Run(backend+"/"+refusal, func(t *testing.T) {
				ctx, request := ownerlessSelectedOperationFixture(t, backend)
				materialized := abandonSelectedOperationPreparation(t, ctx, request)
				probe := &operationRecoveryProbe{SelectedContractForkLifecycle: request.Owner.ports.fork}
				want := "original_provider_failure"
				switch refusal {
				case "failed":
					if err := probe.FailMaterializedSelectedContractExecutionFork(ctx, materialized.ForkRunID, runfork.ForkOperationFailure{Code: want, Reason: "retain original failure"}); err != nil {
						t.Fatal(err)
					}
				case "uncertain_observation":
					// The native uncertain writer is proved separately. This cell
					// exercises the consumer of its canonical observation without SQL.
					probe.uncertain, want = true, "provider_uncertain"
				case "independent_recovery_failure":
					probe.cause, want = errors.New("independent recovery failure"), "independent recovery failure"
				}
				request.Owner.ports.fork = probe
				selected := request.SourceLoader.(SourceArtifactSelectedContractSourceLoader).Store
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				duplicate := *request.ForkOperation
				duplicate.OperationID = uuid.NewString()
				request.ForkOperation = &duplicate
				result, err := ExecuteSelectedContractRunFork(ctx, request)
				if err == nil || !strings.Contains(err.Error(), want) || result.Activation.Activated {
					t.Fatalf("durable refusal became fresh execution: %+v err=%v", result, err)
				}
				if probe.cause != nil && !errors.Is(err, probe.cause) {
					t.Fatalf("independent recovery error was replaced: %v", err)
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("refused replay mutated durable application state: %v", err)
				}
				request.Owner.ports.contexts.mu.Lock()
				remaining := len(request.Owner.ports.contexts.entries)
				request.Owner.ports.contexts.mu.Unlock()
				if remaining != 0 {
					t.Fatalf("refused replay leaked %d preparation entries", remaining)
				}
			})
		}
	}
}
