package runforkexecution

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

const runForkTestRuntimeInstanceID = "22222222-2222-4222-8222-222222222222"

var runForkTestSourceArtifact = mustRunForkTestSourceArtifact()
var runForkTestBundleHash = runForkTestSourceArtifact.BundleHash()

func mustRunForkTestSourceArtifact() *sourceartifact.AdmittedSourceArtifact {
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n"),
	})
	if err != nil {
		panic(err)
	}
	return artifact
}

type runForkTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
	mu      sync.Mutex
	owners  map[startupownership.Store]SelectedContractExecutionOwner
}

var runForkTestWorkFixtures sync.Map

func testGatewayWorkOwner(t testing.TB) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := runForkTestWorkFixtures.Load(t); ok {
		return existing.(*runForkTestWorkFixture).runtime
	}
	fixture := &runForkTestWorkFixture{process: worklifetime.NewProcess()}
	runtimeOwner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "selected-contract-gateway-test",
		BundleHash:        "selected-contract-gateway-bundle",
	})
	if err != nil {
		t.Fatalf("create selected-fork test work owner: %v", err)
	}
	fixture.runtime = runtimeOwner
	actual, loaded := runForkTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*runForkTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer runForkTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire selected-fork test work owner: %v", err)
			return
		}
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join selected-fork test process owner: %v", err)
		}
	})
	return runtimeOwner
}

func runForkTestContext(t testing.TB) context.Context {
	t.Helper()
	ctx := worklifetime.WithOccurrence(context.Background(), testGatewayWorkOwner(t))
	fixture, _ := runForkTestWorkFixtures.Load(t)
	ctx = worklifetime.WithProcess(ctx, fixture.(*runForkTestWorkFixture).process)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	return runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runForkTestRuntimeInstanceID, runForkTestBundleHash),
	)
}

func runForkOriginalContextForTest(t testing.TB, ctx context.Context, capability startupownership.ProcessCapability) context.Context {
	t.Helper()
	original, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	process, found := worklifetime.ProcessFromContext(ctx)
	if !found {
		ownerCtx := runForkTestContext(t)
		process, _ = worklifetime.ProcessFromContext(ownerCtx)
		ctx = worklifetime.WithProcess(ctx, process)
	}
	if err := process.BindOriginalProcess(original.LeaseStamp()); err != nil {
		t.Fatal(err)
	}
	ctx, err = startupownership.WithProcessMutationAuthority(ctx, capability)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func nativeRunForkTestContext(t testing.TB, ctx context.Context, selected any) context.Context {
	t.Helper()
	if startupownership.HasMutationAuthority(ctx) {
		if _, err := startupownership.CaptureMutationAuthority(ctx); err != nil {
			t.Fatal(err)
		}
		return ctx
	}
	owner, ok := selected.(startupownership.Store)
	if !ok {
		t.Fatalf("source fixture requires its original process owner, got %T", selected)
	}
	return runForkOriginalContextForTest(t, ctx, selectedContractTestProcessCapability(t, ctx, owner))
}

func successorRunForkProcessForTest(t testing.TB, ctx context.Context, selected startupownership.Store) (context.Context, *worklifetime.Process, startupownership.ProcessCapability) {
	t.Helper()
	capability, err := selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "selected-contract-successor-test", BootID: uuid.NewString(), RuntimeInstanceID: runForkTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	ctx = runForkOriginalContextForTest(t, worklifetime.WithProcess(ctx, process), capability)
	runtime, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: runForkTestRuntimeInstanceID, BundleHash: runForkTestBundleHash})
	if err != nil {
		t.Fatal(err)
	}
	ctx = worklifetime.WithOccurrence(ctx, runtime)
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := runtime.RetireAndWait(join); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(join); err != nil {
			t.Error(err)
		}
		if err := capability.Release(join); err != nil {
			t.Error(err)
		}
	})
	return ctx, process, capability
}
