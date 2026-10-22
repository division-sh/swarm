package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func proveSelectedCompletionScope(t *testing.T, ordinaryCtx, selectedCtx context.Context, selected any, owner runlifecycle.CandidateOwner, source semanticview.Source, childRunID string) {
	t.Helper()
	requester := selected.(interface {
		RequestCompletionCandidate(context.Context, runlifecycle.CandidateRequest) (runlifecycle.CandidateRequestDisposition, error)
	})
	read := func(scope runlifecycle.CandidateScope) runlifecycle.CandidatePage {
		t.Helper()
		page, err := owner.ListCompletionCandidates(ordinaryCtx, scope, runlifecycle.CandidateCursor{}, 128)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	catalog, err := runtime.RunLifecycleTerminalCatalog(source)
	if err != nil {
		t.Fatal(err)
	}
	authority, present := effects.AuthorityFromContext(selectedCtx)
	if !present || authority.SelectedFork.ForkRunID != childRunID {
		t.Fatal("probe did not capture actual selected publication authority")
	}
	sourceFact, present := correlation.SourceArtifactFactFromContext(selectedCtx)
	if !present {
		t.Fatal("probe did not capture admitted selected source")
	}
	scope := runlifecycle.CandidateScope{BundleHash: sourceFact.BundleHash(), SelectedForkRunID: childRunID}
	page := read(scope)
	if len(page.Candidates) != 1 {
		t.Fatalf("activation candidate missing from exact scope: %+v", page)
	}
	// Clear only through the real fenced algorithm; the non-final receiver
	// must block completion before its input has been published.
	blocked, err := owner.ExecuteCompletionCandidate(selectedCtx, page.Candidates[0], catalog)
	if err != nil || !blocked.Committed || blocked.Outcome != runlifecycle.OutcomeAwaitMutation {
		t.Fatalf("unpublished retained input was completed: %+v %v", blocked, err)
	}
	due := time.Now().UTC().Add(time.Hour)
	if _, err := requester.RequestCompletionCandidate(selectedCtx, runlifecycle.CandidateAtTime(childRunID, due)); err != nil {
		t.Fatal(err)
	}
	page = read(scope)
	if len(page.Candidates) != 1 || page.Candidates[0].SelectedForkRunID != childRunID {
		t.Fatalf("canonical candidate request lost selected mode: %+v", page)
	}
	candidate := page.Candidates[0]
	for _, otherScope := range []runlifecycle.CandidateScope{
		{BundleHash: scope.BundleHash},
		{BundleHash: scope.BundleHash, SelectedForkRunID: uuid.NewString()},
	} {
		if other := read(otherScope); len(other.Candidates) != 0 {
			t.Fatalf("ordinary or sibling enumeration stole selected work: %+v", other)
		}
	}
	before, err := storetest.ReadSelectedForkSourceDomain(ordinaryCtx, selected, childRunID)
	if err != nil {
		t.Fatal(err)
	}
	stale := authority
	stale.FenceGeneration++
	foreign := authority
	foreign.SelectedFork.ForkRunID = uuid.NewString()
	for _, fault := range []struct {
		ctx       context.Context
		candidate runlifecycle.Candidate
	}{
		{ordinaryCtx, candidate},
		{selectedCtx, runlifecycle.Candidate{RunID: candidate.RunID, BundleHash: candidate.BundleHash, Revision: candidate.Revision, DueAt: candidate.DueAt}},
		{effects.WithAuthority(selectedCtx, stale), candidate},
		{effects.WithAuthority(selectedCtx, foreign), candidate},
	} {
		result, err := owner.ExecuteCompletionCandidate(fault.ctx, fault.candidate, catalog)
		if !errors.Is(err, runlifecycle.ErrCompletionAuthority) || result.Committed {
			t.Fatalf("direct completion bypassed scope/fence: %+v %v", result, err)
		}
		if after := read(scope); !reflect.DeepEqual(page, after) {
			t.Fatal("refused completion changed candidate revision or due")
		}
		if after, err := storetest.ReadSelectedForkSourceDomain(ordinaryCtx, selected, childRunID); err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("refused completion changed child domain facts: %v", err)
		}
	}
	result, err := owner.ExecuteCompletionCandidate(selectedCtx, candidate, catalog)
	if err != nil || !result.Committed || result.Outcome != runlifecycle.OutcomeRearmAt || !result.Candidate.SameIdentity(candidate) {
		t.Fatalf("fresh native selected fence rejected current candidate: %+v %v", result, err)
	}
	ordinaryRunID := uuid.NewString()
	storetest.RequireRun(t, ordinaryCtx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
		RunID: ordinaryRunID, State: runlifecycle.StateRunning, Origin: runlifecycle.ScenarioSetupRunOrigin(),
		BundleHash: sourceFact.BundleHash(), StartedAt: time.Now().UTC().Add(-time.Minute),
	})
	if _, err := requester.RequestCompletionCandidate(ordinaryCtx, runlifecycle.CandidateAtTime(ordinaryRunID, due)); err != nil {
		t.Fatal(err)
	}
	ordinaryScope := runlifecycle.CandidateScope{BundleHash: scope.BundleHash}
	ordinary := read(ordinaryScope)
	if len(ordinary.Candidates) != 1 || ordinary.Candidates[0].RunID != ordinaryRunID || ordinary.Candidates[0].SelectedForkRunID != "" {
		t.Fatalf("ordinary same-bundle candidate lost its own namespace: %+v", ordinary)
	}
	wrong, err := owner.ExecuteCompletionCandidate(selectedCtx, ordinary.Candidates[0], catalog)
	if !errors.Is(err, runlifecycle.ErrCompletionAuthority) || wrong.Committed || !reflect.DeepEqual(ordinary, read(ordinaryScope)) {
		t.Fatalf("selected authority consumed ordinary work: %+v %v", wrong, err)
	}
	legitimate, err := owner.ExecuteCompletionCandidate(ordinaryCtx, ordinary.Candidates[0], catalog)
	if err != nil || !legitimate.Committed || legitimate.Outcome != runlifecycle.OutcomeRearmAt || !legitimate.Candidate.SameIdentity(ordinary.Candidates[0]) {
		t.Fatalf("ordinary same-bundle work no longer executes: %+v %v", legitimate, err)
	}
}
