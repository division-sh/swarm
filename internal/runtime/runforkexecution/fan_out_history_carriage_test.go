package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type fanOutHistoryFixtureLoader struct {
	loaded LoadedSelectedContractSource
	err    error
	calls  int
}

func (l *fanOutHistoryFixtureLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	l.calls++
	if err := ctx.Err(); err != nil {
		return LoadedSelectedContractSource{}, err
	}
	if req.SourceRunID != "" || req.BundleHash != req.Selection.BundleHash || req.Selection.Mode != runfork.RunForkContractSelectionModeBundleHash {
		return LoadedSelectedContractSource{}, errors.New("history loader requires an exact immutable artifact, not a current run")
	}
	return l.loaded, l.err
}

func TestForkFanOutHistoryLoadsOnceAndReleasesEveryAcquisition(t *testing.T) {
	archive := semanticview.Wrap(loadRunForkExecutionFixtureBundle(t, "internal/runtime/testdata/generic-swarm-bundle"))
	current := semanticview.Wrap(loadRunForkExecutionFixtureBundle(t, "tests/tier1-primitives/test-emits-multiple"))
	primary, err := semanticview.CompileOriginalLoopCarriage(current)
	if err != nil {
		t.Fatal(err)
	}
	currentBundle, _ := semanticview.Bundle(current)
	archivedBundle, _ := semanticview.Bundle(archive)
	compiled := archive.FanOutPlans()[0]
	source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, Field: strings.TrimPrefix(compiled.ItemsFrom, "payload.")}
	if strings.HasPrefix(compiled.ItemsFrom, "entity.") {
		source.Kind, source.Field = fanoutobligation.SourceEntityField, strings.TrimPrefix(compiled.ItemsFrom, "entity.")
	}
	first := selectedFanOutHistoryFixture(t, compiled, compiled.Ref, source, compiled.SemanticEvidence())
	first.Intent.Status, first.Intent.Cursor = fanoutobligation.StatusClosed, 1
	first.Outcomes = []fanoutobligation.Outcome{{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: uuid.NewString(),
		InheritedDisposition: fanoutobligation.InheritedSucceeded, CreatedAt: first.Intent.CreatedAt}}
	second := first
	second.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
	plan := runfork.RunForkPlan{FanOutObligations: []runfork.RunForkFanOutObligation{first, second}}
	fact, err := correlation.NewSourceArtifactFact(archivedBundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"success", "load_failure", "foreign_fact", "cleanup_failure", "canceled"} {
		t.Run(variant, func(t *testing.T) {
			released := 0
			fault := errors.New("retained source acquisition failed")
			loader := &fanOutHistoryFixtureLoader{loaded: LoadedSelectedContractSource{Source: archive, SourceArtifactFact: fact,
				Cleanup: func() error {
					released++
					if variant == "cleanup_failure" {
						return fault
					}
					return nil
				}}}
			ctx := context.Background()
			switch variant {
			case "load_failure":
				loader.err = fault
			case "foreign_fact":
				loader.loaded.SourceArtifactFact, err = correlation.NewSourceArtifactFact(currentBundle.SourceArtifact.BundleHash())
				if err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			combined, err := retainOriginalFanOutHistory(ctx, loader, primary, currentBundle.SourceArtifact.BundleHash(), plan)
			if variant == "success" {
				if err != nil || loader.calls != 1 || released != 1 {
					t.Fatalf("duplicate history load or leaked projection: calls=%d released=%d err=%v", loader.calls, released, err)
				}
				if _, _, err := combined.ResolveFanOut(compiled.Ref, compiled.Site.Node, compiled.Site.EventType); err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("failed acquisition minted archived meaning")
			} else if variant == "canceled" {
				if !errors.Is(err, context.Canceled) || loader.calls != 0 || released != 0 {
					t.Fatalf("canceled history admission loaded artifacts: calls=%d released=%d err=%v", loader.calls, released, err)
				}
			} else if loader.calls != 1 || released != 1 {
				t.Fatalf("failed acquisition leaked cleanup: calls=%d released=%d err=%v", loader.calls, released, err)
			}
		})
	}
}
