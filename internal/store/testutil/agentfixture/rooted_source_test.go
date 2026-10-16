package agentfixture

import (
	"context"
	"errors"
	"testing"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type fixtureBoundRootStore struct {
	Store
	root  sourceartifact.RootedSource
	err   error
	loads int
}

func (s *fixtureBoundRootStore) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	s.loads++
	if err := ctx.Err(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if runID != "bound-run" {
		return sourceartifact.RootedSource{}, errors.New("unknown run")
	}
	return s.root, s.err
}

func TestS1AgentFixtureCoordinateConsumesBoundRunRoot(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "run-a", sourceartifactfixture.Artifact())
	base := correlation.WithSourceArtifactFact(context.Background(), sourceartifactfixture.FactFor(rooted.Artifact()))
	for _, ctx := range []context.Context{base, correlation.WithRootAdmission(base, rooted.Admission())} {
		selected := &fixtureBoundRootStore{root: rooted}
		for i := 0; i < 2; i++ {
			coordinate, err := fixtureRunCoordinate(ctx, selected, "bound-run", rooted.Admission().BundleHash())
			if err != nil || coordinate.RootReference() != rooted.Admission().Reference() || coordinate.RootDirectory != "run-a" || coordinate.Validate() != nil {
				t.Fatalf("bound run root lost in source coordinate: %+v err=%v", coordinate, err)
			}
		}
		if selected.loads != 2 {
			t.Fatal("coordinate read bypassed the bound-run owner")
		}
	}
}

func TestS1AgentFixtureCoordinateRejectsCrossedClearedOrMissingEvidence(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "run-a", sourceartifactfixture.Artifact())
	other, _ := sourceartifactfixture.Rooted(t, "run-b", sourceartifactfixture.Artifact())
	base := correlation.WithSourceArtifactFact(context.Background(), sourceartifactfixture.FactFor(rooted.Artifact()))
	cancelled, cancel := context.WithCancel(base)
	cancel()
	for _, cell := range []struct {
		name string
		ctx  context.Context
		root sourceartifact.RootedSource
		hash string
	}{
		{"same-hash-foreign-root", correlation.WithRootAdmission(base, other.Admission()), rooted, rooted.Admission().BundleHash()},
		{"explicit-clear", correlation.WithRootAdmission(base, sourceartifact.RootAdmission{}), rooted, rooted.Admission().BundleHash()},
		{"missing-bound-root", base, sourceartifact.RootedSource{}, rooted.Admission().BundleHash()},
		{"different-requested-hash", base, rooted, "bundle-v2:sha256:foreign"},
		{"cancelled", cancelled, rooted, rooted.Admission().BundleHash()},
	} {
		t.Run(cell.name, func(t *testing.T) {
			selected := &fixtureBoundRootStore{root: cell.root}
			coordinate, err := fixtureRunCoordinate(cell.ctx, selected, "bound-run", cell.hash)
			if err == nil || coordinate != (runtimeagenttopology.SourceCoordinate{}) || selected.loads != 1 {
				t.Fatalf("invalid root gained a fixture coordinate: %+v loads=%d err=%v", coordinate, selected.loads, err)
			}
			if cell.name == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("root read discarded cancellation: %v", err)
			}
		})
	}
	fault := errors.New("bound-root read failed")
	if coordinate, err := fixtureRunCoordinate(base, &fixtureBoundRootStore{err: fault}, "bound-run", ""); !errors.Is(err, fault) || coordinate != (runtimeagenttopology.SourceCoordinate{}) {
		t.Fatalf("root read error gained a coordinate: %+v err=%v", coordinate, err)
	}
}
