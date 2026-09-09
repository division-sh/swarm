package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestPreparedSelectedForkMaterializationOwnsSnapshot(t *testing.T) {
	ctx := runForkTestContext(t)
	selection := testContractSelection()
	frontier := testContractFrontierAdmission(selection)
	routes := testSelectedContractRouteAdmission(frontier)
	prepared := &PreparedSelectedFork{
		operation:     selectedContractOperationForTest(t, ctx),
		loadedSource:  testLoadedSelectedSource(selection),
		frontier:      frontier,
		routeTopology: testSelectedContractRouteTopologyFromAdmission(t, frontier, routes),
		model:         testSelectedContractExecutionModel(t, frontier),
		actors:        []runfork.SelectedForkPreparedActor{{SurfaceID: "original-receipt"}},
	}
	defer prepared.Close()
	want, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	copy, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	copy.FrontierAdmission.FrontierEvents[0].EventName = "changed"
	copy.FrontierAdmission.FrontierEvents[0].SourceClassifications = append(copy.FrontierAdmission.FrontierEvents[0].SourceClassifications, "changed")
	copy.RouteTopology.Owner = "changed"
	copy.RecipientPlanning.Owner = "changed"
	copy.Preparation.Actors[0].SurfaceID = "changed"
	got, err := prepared.MaterializationRequest()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("consumer changed preparation: err=%v", err)
	}
	prepared.bound = true
	if _, err := prepared.MaterializationRequest(); err == nil {
		t.Fatal("bound preparation exposed another materialization request")
	}
}

func TestPreparedSelectedForkReleasesInvalidSourceBeforeReturning(t *testing.T) {
	ctx := runForkTestContext(t)
	process, _ := worklifetime.ProcessFromContext(ctx)
	baseline := process.ActiveCount()
	cleanupFailure := errors.New("projection cleanup failed")
	for _, invalid := range []string{"source_fact", "module"} {
		t.Run(invalid, func(t *testing.T) {
			loaded := testLoadedSelectedSource(testContractSelection())
			if invalid == "source_fact" {
				loaded.SourceArtifactFact = LoadedSelectedContractSource{}.SourceArtifactFact
			}
			calls := 0
			loaded.Cleanup = func() error {
				calls++
				if process.ActiveCount() <= baseline {
					t.Error("preparation released process ownership before projection cleanup")
				}
				return cleanupFailure
			}
			owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{contexts: &selectedForkContexts{process: process, recovered: true}}}
			prepared, err := owner.Prepare(ctx, SelectedContractExecutionRequest{
				ExpectedBundleHash: runForkTestBundleHash,
				SourceArtifactFact: testLoadedSelectedSource(testContractSelection()).SourceArtifactFact,
				SourceLoader:       &fakeSelectedContractSourceLoader{loaded: loaded, original: originalActivationSourceFixture(t)}, ContractSelection: testContractSelection(),
			})
			if prepared != nil || !errors.Is(err, cleanupFailure) || calls != 1 {
				t.Fatalf("invalid preparation: prepared=%v err=%v cleanup calls=%d", prepared, err, calls)
			}
			if process.ActiveCount() != baseline {
				t.Fatal("invalid preparation leaked process work")
			}
		})
	}
}

func TestPreparedSelectedForkCanceledMaterializationFailsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(runForkTestContext(t))
	prepared := &PreparedSelectedFork{operation: selectedContractOperationForTest(t, ctx)}
	defer prepared.Close()
	if _, err := prepared.MaterializationRequest(); err == nil || !strings.Contains(err.Error(), "recipient plan") {
		t.Fatalf("missing admitted plan: %v", err)
	}
	cancel()
	if _, err := prepared.MaterializationRequest(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation exposed mutation evidence: %v", err)
	}
}
