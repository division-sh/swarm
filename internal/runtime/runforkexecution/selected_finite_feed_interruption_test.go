package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type selectedInterruptionExecutionStore struct {
	SelectedContractRuntimeExecutionLifecycle
	failures int
	closes   int
}

func (s *selectedInterruptionExecutionStore) FailRunForkSelectedContractRuntimeExecution(context.Context, effects.Authority, json.RawMessage) error {
	s.failures++
	return nil
}

func (s *selectedInterruptionExecutionStore) CloseRunForkSelectedContractRuntimeExecution(context.Context, string) error {
	s.closes++
	return nil
}

func TestSelectedFiniteFeedProcessRetirementPreservesRecoverablePredecessor(t *testing.T) {
	for _, cell := range []struct {
		name         string
		kind         runfork.RunForkPointKind
		cause        error
		wantFailures int
		wantCloses   int
	}{
		{"deployment-retired", runfork.RunForkPointDeploymentRevision, worklifetime.ErrRetired, 0, 0},
		{"deployment-failed", runfork.RunForkPointDeploymentRevision, errors.New("business failure"), 1, 1},
		{"event-retired", runfork.RunForkPointEvent, worklifetime.ErrRetired, 1, 1},
	} {
		t.Run(cell.name, func(t *testing.T) {
			owner, _, ctx := contextLifetimeOwner(t)
			prepared := failureDiscardPreparation(t, owner, ctx)
			bindContextLifetimePreparation(t, owner, prepared)
			evidence, err := prepared.processOwner.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			identity := prepared.operation.selected.Identity()
			authority := effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: identity.ExecutionID,
				ExecutionOwner: evidence.OwnerID, FenceGeneration: evidence.AuthorityGeneration, LeaseExpiresAt: time.Now().Add(time.Minute), ExecutionMode: effects.ExecutionModeLive,
				SelectedFork: effects.SelectedContractForkAuthority{ExecutionID: identity.ExecutionID, ForkRunID: identity.RunID, Generation: identity.Generation,
					AdmissionFingerprint: strings.Repeat("1", 64), ContainerPlanFingerprint: strings.Repeat("2", 64), ActorCensusFingerprint: strings.Repeat("3", 64), EffectiveConfigFingerprint: strings.Repeat("4", 64)}}
			store := &selectedInterruptionExecutionStore{}
			container := selectedContractForkLocalRuntimeContainer{
				proof: SelectedContractForkLocalRuntimeContainer{ForkPoint: runfork.RunForkPoint{Kind: cell.kind}},
				ports: &selectedContractExecutionPorts{runtimeExecution: store},
				req:   publishSelectedContractForkEventsRequest{Prepared: prepared}, authority: authority,
				preparation: runfork.SelectedForkPreparationBinding{SelectedForkPreparation: prepared.evidence(), ForkRunID: identity.RunID},
			}
			err = container.Fail(context.Background(), cell.cause)
			if cell.wantFailures == 0 {
				if !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("retirement was not retained for recovery: %v", err)
				}
			} else if err != nil {
				t.Fatalf("ordinary failure settlement: %v", err)
			}
			if store.failures != cell.wantFailures || store.closes != cell.wantCloses {
				t.Fatalf("failure/close writes=%d/%d, want %d/%d", store.failures, store.closes, cell.wantFailures, cell.wantCloses)
			}
		})
	}
}
