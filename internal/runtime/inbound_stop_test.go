package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func TestStoredInboundCustodyDoesNotMintExecutionAndJoinsEnteredRequests(t *testing.T) {
	catalog, source := runtimeAdmissionTestCatalog(t, "stop", "inbound")
	definition := runtimeAdmissionTestContext(t, source, catalog, eventtest.UUID(t.Name()))
	manager, err := newTestRuntimeContextManager(t, nil, definition)
	if err != nil {
		t.Fatal(err)
	}
	target := definition.StandingTargets[0]
	requestCtx, finishRequest, admitted := manager.BeginInboundRequest(t.Context())
	if !admitted || !manager.InboundClaimAdmissionOpen() {
		t.Fatal("ordinary request admission refused")
	}
	t.Cleanup(finishRequest)
	ordinary, _, err := manager.AcquireInboundPublication(requestCtx, target.Alias, target.Provider)
	if err != nil || ordinary == nil || ordinary.executed == nil {
		t.Fatalf("ordinary execution custody: use=%v err=%v", ordinary, err)
	}
	if err := ordinary.Done(); err != nil {
		t.Fatal(err)
	}
	if err := manager.SealInboundExecution(t.Context()); err != nil {
		t.Fatal(err)
	}
	if manager.InboundClaimAdmissionOpen() {
		t.Fatal("claim completion remained admitted during stop")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if use, _, err := manager.AcquireInboundPublication(canceled, target.Alias, target.Provider); use != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("preparation barrier bypass: use=%v err=%v", use, err)
	}
	if err := manager.OpenStoredInbound(); err != nil {
		t.Fatal(err)
	}
	definition.Runtime.CloseAdmission()
	stored, lookup, err := manager.AcquireInboundPublication(requestCtx, target.Alias, target.Provider)
	if err != nil || stored == nil || stored.executed != nil || !lookup.Loaded() || lookup.Target.RunID != target.RunID {
		t.Fatalf("publication-only custody: use=%v lookup=%+v err=%v", stored, lookup, err)
	}
	if definition.WorkOwner.ActiveCount() != 0 {
		t.Fatal("store-only custody minted an execution lease")
	}
	if _, rooted, err := runtimecorrelation.RootAdmissionFromContext(stored.ctx); !rooted || err != nil {
		t.Fatal("publication custody lost its original rooted source")
	}
	if err := stored.Done(); err != nil {
		t.Fatal(err)
	}
	foreignFact, err := runtimecorrelation.NewSourceArtifactFact(runtimeContextTestHashC)
	if err != nil {
		t.Fatal(err)
	}
	foreign := runtimecorrelation.WithSourceArtifactFact(requestCtx, foreignFact)
	if use, _, err := manager.AcquireInboundPublication(foreign, target.Alias, target.Provider); use != nil || err == nil {
		t.Fatal("store-only admission accepted another source")
	}
	if use, _, err := manager.AcquireIngress(requestCtx, target.Alias, target.Provider); use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("publication phase reopened execution: %v", err)
	}
	joined := make(chan error, 1)
	go func() { joined <- manager.CloseInboundRequests(t.Context()) }()
	finishRequest()
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	if _, release, admitted := manager.BeginInboundRequest(t.Context()); admitted || release != nil {
		t.Fatal("request admitted after listener cutoff")
	}
	if err := manager.OpenStoredInbound(); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("closed intake reopened: %v", err)
	}
}
