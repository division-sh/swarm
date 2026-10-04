package runforkexecution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type countedSourceInspectionStore struct {
	fakeSourceArtifactSelectedContractSourceStore
	artifactReads, availabilityReads int
}

func (s *countedSourceInspectionStore) GetSourceArtifact(ctx context.Context, hash string) (sourceartifact.Persisted, error) {
	s.artifactReads++
	return s.fakeSourceArtifactSelectedContractSourceStore.GetSourceArtifact(ctx, hash)
}

func (s *countedSourceInspectionStore) LoadRunBundleAvailability(ctx context.Context, runID string) (runbundle.Availability, error) {
	s.availabilityReads++
	return s.fakeSourceArtifactSelectedContractSourceStore.LoadRunBundleAvailability(ctx, runID)
}

func TestSelectedSourceInspectionSharesBootCompilerWithoutMaterialization(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	root := t.TempDir()
	temporaryRoot := os.TempDir()
	for _, mode := range []string{runfork.RunForkContractSelectionModeSelectedContracts, runfork.RunForkContractSelectionModeBundleHash} {
		t.Run(mode, func(t *testing.T) {
			runID := uuid.NewString()
			reader := &countedSourceInspectionStore{fakeSourceArtifactSelectedContractSourceStore: fakeSourceArtifactSelectedContractSourceStore{
				availability: runbundle.Availability{RunID: runID, Status: "running", BundleHash: record.BundleHash, SourceArtifactPresent: true}, record: record,
			}}
			loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: reader}
			request := SelectedContractSourceLoadRequest{SourceRunID: runID, BundleHash: record.BundleHash, Selection: runfork.RunForkContractSelection{Mode: mode}}
			if mode == runfork.RunForkContractSelectionModeBundleHash {
				request.Selection.BundleHash = record.BundleHash
			}
			missing := filepath.Join(root, mode, "not-created")
			t.Setenv("TMPDIR", missing)
			inspected, err := loader.InspectRunForkSelectedContractSourceForRequest(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if inspected.Cleanup != nil || inspected.RuntimeProjection != nil || inspected.Source == nil || inspected.Module == nil {
				t.Fatalf("inspection acquired an execution projection or omitted source: %+v", inspected)
			}
			if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("inspection created filesystem state: %v", err)
			}
			wantAvailabilityReads := 0
			if mode == runfork.RunForkContractSelectionModeSelectedContracts {
				wantAvailabilityReads = 1
			}
			if reader.artifactReads != 1 || reader.availabilityReads != wantAvailabilityReads {
				t.Fatalf("inspection reread immutable source: artifact=%d availability=%d", reader.artifactReads, reader.availabilityReads)
			}
			t.Setenv("TMPDIR", temporaryRoot)
			loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			projectionRoot := loaded.RuntimeProjection.PrivateRoot()
			if loaded.Cleanup == nil || projectionRoot == "" || loaded.SourceArtifactFact.BundleHash() != inspected.SourceArtifactFact.BundleHash() ||
				!reflect.DeepEqual(loaded.EffectiveSourceIdentity, inspected.EffectiveSourceIdentity) {
				t.Fatalf("boot/inspection compiler identity differs: inspected=%+v loaded=%+v", inspected.EffectiveSourceIdentity, loaded.EffectiveSourceIdentity)
			}
			if err := loaded.Cleanup(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(projectionRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("boot projection removal unconfirmed: %v", err)
			}
		})
	}
}

func TestSelectedSourceInspectionRejectsMissingCorruptCrossedAndCanceledEvidence(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	for _, tc := range []struct {
		name   string
		mutate func(*fakeSourceArtifactSelectedContractSourceStore)
	}{
		{"missing", func(s *fakeSourceArtifactSelectedContractSourceStore) { s.recordErr = sourceartifact.ErrNotFound }},
		{"read_error", func(s *fakeSourceArtifactSelectedContractSourceStore) {
			s.recordErr = errors.New("required artifact read failed")
		}},
		{"corrupt", func(s *fakeSourceArtifactSelectedContractSourceStore) { s.record.SourceBlob = []byte("corrupt") }},
		{"crossed", func(s *fakeSourceArtifactSelectedContractSourceStore) { s.record.BundleHash = runForkTestBundleHash }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeSourceArtifactSelectedContractSourceStore{record: record}
			tc.mutate(store)
			loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: store}
			request := SelectedContractSourceLoadRequest{BundleHash: record.BundleHash,
				Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash}}
			for _, load := range []func(context.Context, SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error){loader.InspectRunForkSelectedContractSourceForRequest, loader.LoadRunForkSelectedContractSourceForRequest} {
				loaded, err := load(context.Background(), request)
				if err == nil || loaded.RuntimeProjection != nil || loaded.Cleanup != nil {
					t.Fatalf("invalid source admitted/materialized: %+v err=%v", loaded, err)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &countedSourceInspectionStore{}
	if _, err := (SourceArtifactSelectedContractSourceLoader{Store: store}).InspectRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{}); !errors.Is(err, context.Canceled) || store.artifactReads != 0 || store.availabilityReads != 0 {
		t.Fatalf("canceled inspection reached source reads: err=%v artifact=%d availability=%d", err, store.artifactReads, store.availabilityReads)
	}
}
