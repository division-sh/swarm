package runforkexecution

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type responsibilityReader struct {
	fakeSourceArtifactSelectedContractSourceStore
	read func(string, string) (sourceartifact.RootedSource, error)
}

func TestS1LoadedSelectedSourceRefusesModuleFromAnotherRoot(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	request := SelectedContractSourceLoadRequest{
		Responsibility: FreshSelectedSource(record.BundleHash), BundleHash: record.BundleHash,
		Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash},
	}
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: &fakeSourceArtifactSelectedContractSourceStore{record: record}}
	first, err := loader.InspectRunForkSelectedContractSourceForRequest(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := sourceartifactfixture.Rooted(t, "other", record.rooted.Artifact())
	record.rooted = other
	loader.Store = &fakeSourceArtifactSelectedContractSourceStore{record: record}
	second, err := loader.InspectRunForkSelectedContractSourceForRequest(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.RootedSource.Admission().Reference() == second.RootedSource.Admission().Reference() {
		t.Fatal("module control requires distinct roots for the same content")
	}
	first.Module = second.Module
	if err := validateLoadedSelectedContractSource(request, first); err == nil {
		t.Fatal("selected source admitted an executable module compiled under another root")
	}
}

func (s responsibilityReader) LoadRunRootedSource(_ context.Context, id string) (sourceartifact.RootedSource, error) {
	return s.read("original", id)
}

func (s responsibilityReader) LoadSelectedForkRootedSource(_ context.Context, id string) (sourceartifact.RootedSource, error) {
	return s.read("retained", id)
}

func (s responsibilityReader) GetRootedSource(_ context.Context, ref sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error) {
	return s.read("selected", ref.RootDirectory)
}

func (s responsibilityReader) ResolveUnboundRootedSource(_ context.Context, hash string) (sourceartifact.RootedSource, error) {
	return s.read("unbound", hash)
}

func TestS1SelectedSourceResponsibilitiesDoNotSubstituteOwners(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	original, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	target, _ := sourceartifactfixture.Rooted(t, "target", original.Artifact())
	if original.Admission().Reference() == target.Admission().Reference() || original.Admission().BundleHash() != target.Admission().BundleHash() {
		t.Fatal("fixture must have distinct roots for identical content")
	}
	runID, childID := uuid.NewString(), uuid.NewString()
	fact, err := correlation.NewSourceArtifactFact(target.Admission().BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	selected := correlation.WithRootAdmission(correlation.WithSourceArtifactFact(context.Background(), fact), target.Admission())
	for _, tc := range []struct {
		name               string
		request            SelectedSourceResponsibility
		ctx                context.Context
		wantOwner, wantKey string
		want               sourceartifact.RootedSource
	}{
		{"original", OriginalRunSource(runID), selected, "original", runID, original},
		{"retained", RetainedForkSource(childID), context.Background(), "retained", childID, target},
		{"fresh_selected", FreshSelectedSource(fact.BundleHash()), selected, "selected", target.Admission().RootDirectory(), target},
		{"fresh_unbound", FreshSelectedSource(fact.BundleHash()), context.Background(), "unbound", fact.BundleHash(), target},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			reader := responsibilityReader{read: func(owner, key string) (sourceartifact.RootedSource, error) {
				calls = append(calls, owner+":"+key)
				return tc.want, nil
			}}
			got, err := tc.request.load(tc.ctx, &reader)
			if err != nil || got.Admission().Reference() != tc.want.Admission().Reference() || !reflect.DeepEqual(calls, []string{tc.wantOwner + ":" + tc.wantKey}) {
				t.Fatalf("responsibility substitution: root=%+v calls=%v err=%v", got.Admission().Reference(), calls, err)
			}
		})
	}
	for _, tc := range []struct {
		name    string
		request SelectedSourceResponsibility
		ctx     context.Context
	}{
		{"missing_role", SelectedSourceResponsibility{}, selected},
		{"bad_original_id", OriginalRunSource("not-a-run"), selected},
		{"bad_retained_id", RetainedForkSource(uuid.Nil.String()), selected},
		{"bad_fresh_hash", FreshSelectedSource("not-a-hash"), selected},
		{"cleared_context", FreshSelectedSource(fact.BundleHash()), correlation.WithRootAdmission(selected, sourceartifact.RootAdmission{})},
		{"missing_content_fact", FreshSelectedSource(fact.BundleHash()), correlation.WithRootAdmission(context.Background(), target.Admission())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := responsibilityReader{read: func(owner, key string) (sourceartifact.RootedSource, error) {
				t.Fatalf("invalid request reached %s %s", owner, key)
				return target, nil
			}}
			got, err := tc.request.load(tc.ctx, &reader)
			if err == nil || got.Validate() == nil {
				t.Fatalf("invalid responsibility gained source: root=%+v err=%v", got.Admission().Reference(), err)
			}
		})
	}
	t.Run("crossed_same_content_root", func(t *testing.T) {
		reader := responsibilityReader{read: func(_, _ string) (sourceartifact.RootedSource, error) { return original, nil }}
		got, err := FreshSelectedSource(fact.BundleHash()).load(selected, &reader)
		if err == nil || got.Validate() == nil {
			t.Fatalf("same-content crossed root admitted: %+v err=%v", got.Admission().Reference(), err)
		}
	})
	t.Run("cancel_after_read_preserves_cause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cause := errors.New("retained native read failed")
		reader := responsibilityReader{read: func(_, _ string) (sourceartifact.RootedSource, error) {
			cancel()
			return target, cause
		}}
		got, err := RetainedForkSource(childID).load(ctx, &reader)
		if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || got.Validate() == nil {
			t.Fatalf("read cancellation disclosed source or lost cause: %+v err=%v", got.Admission().Reference(), err)
		}
	})
}
