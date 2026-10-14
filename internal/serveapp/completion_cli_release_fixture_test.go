package serveapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type completion2270ReleaseFixture struct {
	proof    *completion2270InvocationProof
	selected *completion2270ObservedWorkspace
	root     string
	args     []string
	attempt  string
	reads    int
	releases int
	disposed bool
	readErr  error
	closeErr error
}

func newCompletion2270ReleaseFixture(t *testing.T, proof *completion2270InvocationProof) *completion2270ReleaseFixture {
	t.Helper()
	f := &completion2270ReleaseFixture{proof: proof}
	f.root, f.args, f.attempt = completion2270ProtocolFixture(t, "")
	sourceRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceRoot, "schema.yaml"), []byte("name: selected-witness\n"), 0600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(sourceRoot)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := projection.Release(); err != nil {
			t.Error(err)
		}
	})
	template := &completion2270ObservedWorkspace{DockerManager: workspace.NewDockerManager(), proof: proof}
	template.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) == 0 {
			return "", errors.New("empty Docker command")
		}
		if args[0] == "container" {
			f.releases++
			f.disposed = true
			return "", f.closeErr
		}
		if args[0] != "exec" || len(args) != 7 || args[6] != completion2270InvocationFile {
			return "", fmt.Errorf("foreign witness read: %v", args)
		}
		if f.disposed {
			return "", errors.New("disposed selected workspace was reopened")
		}
		f.reads++
		if f.readErr != nil {
			return "", f.readErr
		}
		containers := f.selected.SystemWorkspaceContainers()
		if args[1] == containers[1] {
			return "", nil
		}
		if args[1] != containers[0] {
			return "", errors.New("read outside exact rebound owner")
		}
		raw, err := os.ReadFile(filepath.Join(f.root, completion2270InvocationFile))
		if os.IsNotExist(err) {
			return "", nil
		}
		return string(raw), err
	})
	lifecycle, err := template.RebindSourceProjection(projection, semanticview.Wrap(&contracts.WorkflowContractBundle{}))
	if err != nil {
		t.Fatal(err)
	}
	f.selected = lifecycle.(*completion2270ObservedWorkspace)
	return f
}

func newCompletion2270PublishedProof(t *testing.T) *completion2270InvocationProof {
	t.Helper()
	contexts, err := runtimepkg.NewRuntimeContextManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: published-witness\n"), 0600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	owner := newSupervisorTestRuntimeOccurrence(t, artifact.BundleHash())
	source := semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{})
	fact := mustServeTestEphemeralSourceArtifactFact(artifact.BundleHash())
	publisher, err := bus.NewEphemeralEventBusWithOptions(nil, bus.EventBusOptions{
		SourceArtifactFact: fact, WorkOwner: owner, ContractBundle: source, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager := workspace.NewDockerManager()
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		if len(args) != 7 || args[0] != "exec" {
			return "", fmt.Errorf("published observation changed: %v", args)
		}
		return "", nil
	})
	if err := contexts.Register(runtimepkg.BundleContext{SourceArtifactFact: fact, Source: source, WorkOwner: owner,
		Runtime: &runtimepkg.Runtime{Bus: publisher, Workspace: &completion2270ObservedWorkspace{DockerManager: manager}},
	}); err != nil {
		t.Fatal(err)
	}
	proof := &completion2270InvocationProof{}
	proof.observe(contexts)
	return proof
}

func (f *completion2270ReleaseFixture) invoke(t *testing.T, business bool) {
	t.Helper()
	args, prompt := f.args, completion2270StartupPrompt
	if business {
		args, prompt = append(append([]string{}, args...), "--model", "request-A"), "business request"
	}
	if _, stderr, err := runCompletion2270Protocol(t, f.root, args, prompt); err != nil {
		t.Fatalf("physical invocation failed: %v %s", err, stderr)
	}
}

func TestClaudeCLICompletion2270SelectedWitnessRelease(t *testing.T) {
	proof := newCompletion2270PublishedProof(t)
	f := newCompletion2270ReleaseFixture(t, proof)
	f.invoke(t, true)
	if loaded := proof.contexts.LoadedContexts(); len(loaded) != 1 || loaded[0].BundleHash() == f.selected.SourceProjectionBundleHash() {
		t.Fatal("selected owner was not distinct from the published population")
	}
	if rows, err := proof.read(t.Context()); err != nil || len(rows) != 0 {
		t.Fatalf("unreleased selected evidence invented: %+v %v", rows, err)
	}
	if err := f.selected.ReleaseSourceProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	rows, err := proof.read(t.Context())
	if err != nil || !f.disposed || f.releases != 1 || f.reads != 2 || validateCompletion2270SingleInvocation(rows, f.attempt) != nil {
		t.Fatalf("selected witness not captured before real disposal: %+v reads=%d releases=%d err=%v", rows, f.reads, f.releases, err)
	}
}

func TestClaudeCLICompletion2270SelectedDecorationPreservesConcretePreflight(t *testing.T) {
	manager := workspace.NewDockerManager()
	req := selectedAPICapabilityRequest{Workspaces: manager}
	proof := &completion2270InvocationProof{}
	selected, err := completion2270ObserveSelectedWorkspace(req, proof)
	if err != nil {
		t.Fatal(err)
	}
	if concrete, ok := req.Workspaces.(*workspace.DockerManager); !ok || concrete != manager {
		t.Fatal("selected decoration changed the concrete ordinary/preflight owner")
	}
	observed, ok := selected.Workspaces.(*completion2270ObservedWorkspace)
	if !ok || observed.DockerManager != manager || observed.proof != proof || observed.selected {
		t.Fatal("selected request did not retain the actual undecorated owner")
	}
	for _, lifecycle := range []cliapp.ServeWorkspaceLifecycle{nil, workspace.NewHostManager(), observed} {
		if _, err := completion2270ObserveSelectedWorkspace(selectedAPICapabilityRequest{Workspaces: lifecycle}, proof); err == nil {
			t.Fatal("selected observation admitted missing, non-Docker or duplicate decoration")
		}
	}
	if _, err := completion2270ObserveSelectedWorkspace(req, nil); err == nil {
		t.Fatal("selected observation admitted missing proof owner")
	}
}

func TestClaudeCLICompletion2270SelectedWitnessImmutableAfterDisposal(t *testing.T) {
	proof := newCompletion2270PublishedProof(t)
	f := newCompletion2270ReleaseFixture(t, proof)
	f.invoke(t, true)
	if err := f.selected.ReleaseSourceProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	rows, err := proof.read(t.Context())
	if err != nil || validateCompletion2270SingleInvocation(rows, f.attempt) != nil {
		t.Fatalf("snapshot missing: %+v %v", rows, err)
	}
	if err := os.Remove(filepath.Join(f.root, completion2270InvocationFile)); err != nil {
		t.Fatal(err)
	}
	rows[0].AttemptID, rows[0].Model = "mutated", "mutated"
	again, err := proof.read(t.Context())
	if err != nil || validateCompletion2270SingleInvocation(again, f.attempt) != nil || f.reads != 2 {
		t.Fatalf("snapshot aliased or reopened disposed authority: %+v %v", again, err)
	}
}

func TestClaudeCLICompletion2270SelectedWitnessFailureStillDisposes(t *testing.T) {
	for _, failure := range []string{"missing", "read", "malformed", "read_and_cleanup"} {
		t.Run(failure, func(t *testing.T) {
			proof := newCompletion2270PublishedProof(t)
			f := newCompletion2270ReleaseFixture(t, proof)
			if failure != "missing" {
				f.invoke(t, true)
			}
			if failure == "malformed" {
				if err := os.WriteFile(filepath.Join(f.root, completion2270InvocationFile), []byte("{broken"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "read" || failure == "read_and_cleanup" {
				f.readErr = errors.New("witness read refused")
			}
			if failure == "read_and_cleanup" {
				f.closeErr = errors.New("real disposal refused")
			}
			releaseErr := f.selected.ReleaseSourceProjection(t.Context())
			rows, err := proof.read(t.Context())
			if !f.disposed || f.releases != 1 || (err == nil && validateCompletion2270SingleInvocation(rows, f.attempt) == nil) {
				t.Fatalf("bad witness became success or skipped cleanup: rows=%+v err=%v release=%v", rows, err, releaseErr)
			}
			if failure != "missing" && releaseErr == nil {
				t.Fatal("capture failure was hidden by cleanup")
			}
			if f.closeErr != nil && (!errors.Is(releaseErr, f.readErr) || !errors.Is(releaseErr, f.closeErr)) {
				t.Fatalf("capture/cleanup errors were not joined: %v", releaseErr)
			}
			f.closeErr = nil
			retryErr := f.selected.ReleaseSourceProjection(t.Context())
			if failure == "read_and_cleanup" && (!errors.Is(retryErr, f.readErr) || f.releases != 2) {
				t.Fatalf("cleanup retry erased the read error or skipped disposal: %v", retryErr)
			}
		})
	}
}

func TestClaudeCLICompletion2270SelectedWitnessRejectsRepeatedInvocations(t *testing.T) {
	for _, occurrences := range []int{1, 2} {
		t.Run(fmt.Sprint(occurrences), func(t *testing.T) {
			proof := newCompletion2270PublishedProof(t)
			f := newCompletion2270ReleaseFixture(t, proof)
			f.invoke(t, true)
			if occurrences == 1 {
				f.invoke(t, true)
			}
			if err := f.selected.ReleaseSourceProjection(t.Context()); err != nil {
				t.Fatal(err)
			}
			if occurrences == 2 {
				successor := newCompletion2270ReleaseFixture(t, proof)
				for i := 0; i+1 < len(successor.args); i++ {
					if successor.args[i] == "--session-id" {
						successor.args[i+1] = f.attempt
					}
				}
				successor.attempt = f.attempt
				successor.invoke(t, true)
				if err := successor.selected.ReleaseSourceProjection(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			rows, err := proof.read(t.Context())
			if err != nil || len(rows) != 2 || rows[0].AttemptID != f.attempt || rows[1].AttemptID != f.attempt || rows[0].InvocationID == rows[1].InvocationID || validateCompletion2270SingleInvocation(rows, f.attempt) == nil {
				t.Fatalf("second physical invocation was lost: %+v %v", rows, err)
			}
		})
	}
}

func TestClaudeCLICompletion2270SelectedWitnessReadAndReleaseAreIdempotent(t *testing.T) {
	proof := newCompletion2270PublishedProof(t)
	f := newCompletion2270ReleaseFixture(t, proof)
	f.invoke(t, true)
	if err := f.selected.ReleaseSourceProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, err := proof.read(t.Context())
	if err != nil || validateCompletion2270SingleInvocation(first, f.attempt) != nil {
		t.Fatalf("initial snapshot: %+v %v", first, err)
	}
	if err := f.selected.ReleaseSourceProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		rows, err := proof.read(t.Context())
		if err != nil || !reflect.DeepEqual(rows, first) || f.reads != 2 || f.releases != 1 {
			t.Fatalf("read/release duplicated physical evidence: %+v %v", rows, err)
		}
	}
}

func TestClaudeCLICompletion2270SelectedStartupHasNoBusinessWitness(t *testing.T) {
	proof := newCompletion2270PublishedProof(t)
	f := newCompletion2270ReleaseFixture(t, proof)
	f.invoke(t, false)
	if err := f.selected.ReleaseSourceProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	if rows, err := proof.read(t.Context()); err != nil || len(rows) != 0 || !f.disposed {
		t.Fatalf("startup became business or skipped disposal: %+v %v", rows, err)
	}
	if _, err := os.Stat(filepath.Join(f.root, completion2270InvocationFile)); !os.IsNotExist(err) {
		t.Fatalf("startup created a physical business witness: %v", err)
	}
}
