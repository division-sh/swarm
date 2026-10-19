package runforkexecution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type activityHistoryFixtureLoader struct {
	loaded   LoadedSelectedContractSource
	err      error
	requests []SelectedContractSourceLoadRequest
}

func (l *activityHistoryFixtureLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	l.requests = append(l.requests, req)
	return l.loaded, errors.Join(l.err, ctx.Err())
}

func TestActivityHistoryLoadsExactOriginAndReleasesEveryAcquisition(t *testing.T) {
	source := semanticview.Wrap(loadRunForkExecutionFixtureBundle(t, "tests/tier1-primitives/test-emits-multiple"))
	primary, err := semanticview.CompileOriginalLoopCarriage(source)
	if err != nil {
		t.Fatal(err)
	}
	bundle, _ := semanticview.Bundle(source)
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	var origins []semanticview.ActivityRequestOrigin
	var ids []string
	for range 2 {
		id := uuid.NewString()
		request := eventtest.RuntimeControl(id, "platform.activity_requested", "activity-runtime", "", []byte(`{"input":{}}`), 0,
			runID, "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
		origins = append(origins, semanticview.ActivityRequestOrigin{Request: request, Cuts: []semanticview.ActivityRequestCut{{RunID: runID, RequestID: id, Revision: 4, Source: fact}}})
		ids = append(ids, id)
	}
	plan, err := (runfork.RunForkPlan{SourceRunID: runID}).WithHistoricalEvents(4, ids).WithActivityRequestOrigins(4, origins)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"exact", "load_failure", "foreign_fact", "absent_source", "cleanup_failure", "canceled_before", "canceled_during"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fault := errors.New("original artifact acquisition fault")
			released := 0
			loader := &activityHistoryFixtureLoader{loaded: LoadedSelectedContractSource{Source: source, SourceArtifactFact: fact, Cleanup: func() error {
				released++
				if scenario == "canceled_during" {
					cancel()
				}
				if scenario == "cleanup_failure" {
					return fault
				}
				return nil
			}}}
			switch scenario {
			case "load_failure":
				loader.err = fault
			case "foreign_fact":
				loader.loaded.SourceArtifactFact = correlation.SourceArtifactFact{}
			case "absent_source":
				loader.loaded.Source = nil
			case "canceled_before":
				cancel()
			}
			_, err := retainOriginalActivityHistory(ctx, loader, primary, plan)
			if scenario == "exact" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatalf("%s minted original declaration evidence", scenario)
			}
			if scenario == "canceled_before" {
				if !errors.Is(err, context.Canceled) || len(loader.requests) != 0 || released != 0 {
					t.Fatalf("canceled preparation acquired resources: requests=%d released=%d err=%v", len(loader.requests), released, err)
				}
				return
			}
			if len(loader.requests) != 1 || released != 1 {
				t.Fatalf("acquisition was repeated or leaked: requests=%d released=%d err=%v", len(loader.requests), released, err)
			}
			request := loader.requests[0]
			if request.SourceRunID != runID || !request.SourceArtifactFact.Matches(fact) || request.BundleHash != fact.BundleHash() ||
				request.Selection.Mode != runfork.RunForkContractSelectionModeBundleHash || request.Selection.BundleHash != fact.BundleHash() {
				t.Fatalf("loader lost original run-bound artifact coordinates: %+v", request)
			}
			if scenario == "canceled_during" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation after cleanup was hidden: %v", err)
			}
		})
	}
}
