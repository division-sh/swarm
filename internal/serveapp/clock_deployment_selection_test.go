package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type standingSelectionProof struct {
	sets       int
	individual []pipeline.StandingServiceCandidate
	err        error
}

func (p *standingSelectionProof) ReconcileStandingServiceSet(context.Context, []pipeline.StandingServiceCandidate) ([]pipeline.StandingServiceReconciliation, error) {
	p.sets++
	return nil, p.err
}

func (p *standingSelectionProof) ReconcileStandingService(_ context.Context, candidate pipeline.StandingServiceCandidate) (pipeline.StandingServiceReconciliation, error) {
	p.individual = append(p.individual, candidate)
	return pipeline.StandingServiceReconciliation{ServiceID: candidate.ServiceID, RunID: "existing-independent-run"}, p.err
}

func TestFiniteStandingReconciliationCannotOwnDeclarationAbsence(t *testing.T) {
	independent := pipeline.StandingServiceCandidate{ServiceID: "independent-standing", FlowPath: "legacy"}
	for _, deployment := range []bool{false, true} {
		for _, candidates := range [][]pipeline.StandingServiceCandidate{nil, {independent}} {
			owner := &standingSelectionProof{}
			result, err := reconcileStandingServiceCandidates(t.Context(), owner, candidates, deployment)
			if err != nil {
				t.Fatal(err)
			}
			if deployment {
				if owner.sets != 1 || len(owner.individual) != 0 {
					t.Fatal("deployment lost its complete set/removal owner")
				}
				continue
			}
			if owner.sets != 0 || len(owner.individual) != len(candidates) || len(result) != len(candidates) {
				t.Fatalf("finite host inferred declaration absence: owner=%+v result=%+v", owner, result)
			}
			if len(candidates) != 0 && !reflect.DeepEqual(owner.individual[0], independent) {
				t.Fatal("finite host changed an independent standing candidate")
			}
		}
	}
	fault := errors.New("independent standing reconciliation failed")
	owner := &standingSelectionProof{err: fault}
	result, err := reconcileStandingServiceCandidates(t.Context(), owner, []pipeline.StandingServiceCandidate{independent}, false)
	if !errors.Is(err, fault) || result != nil || owner.sets != 0 || len(owner.individual) != 1 {
		t.Fatalf("finite host suppressed failure or fell back to whole-set authority: result=%+v owner=%+v err=%v", result, owner, err)
	}
}
