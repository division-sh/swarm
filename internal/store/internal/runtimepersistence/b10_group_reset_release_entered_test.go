package runtimepersistence

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// This probe only pauses the actual selected-store Release and optionally
// preserves a post-release error. It never fabricates a successful release.
type b10EnteredRelease struct {
	pipelineobligation.Store
	entered chan pipelineobligation.Claim
	resume  chan struct{}
	fault   error
	calls   atomic.Int32
}

func (p *b10EnteredRelease) Release(ctx context.Context, claim pipelineobligation.Claim) error {
	p.calls.Add(1)
	p.entered <- claim
	<-p.resume
	return errors.Join(p.Store.Release(ctx, claim), p.fault)
}

type b10RetirementValidation struct {
	pipelineobligation.PublicationGroup
	validated chan struct{}
}

func (p *b10RetirementValidation) ValidateCommitted(ctx context.Context, claims []pipelineobligation.Claim) error {
	err := p.PublicationGroup.ValidateCommitted(ctx, claims)
	close(p.validated)
	return err
}

func TestB10GroupRetirementJoinsResetReleaseAlreadyEntered(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"healthy", "release_cleanup_error"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 1)
				releases := &b10EnteredRelease{Store: f.store(), entered: make(chan pipelineobligation.Claim, 2), resume: make(chan struct{})}
				if phase == "release_cleanup_error" {
					releases.fault = errors.New("b10 exact release cleanup failure")
				}
				_, bundle := fanOutMixedRouteSource(t)
				var err error
				f.bus, err = newStoreTestEventBus(t, f.raw.(storeTestDurableEventBusStore), bus.EventBusOptions{
					ContractBundle: semanticview.Wrap(bundle), SourceArtifactFact: mustStoreTestSourceArtifactFact(f.seed.bundleHash),
					RuntimeInstanceID: authorActivityTestRuntimeInstanceID, PipelineObligations: releases,
				})
				if err != nil {
					t.Fatal(err)
				}
				values := stageB10RetirementGroup(t, f)
				before := f.snapshot(t)
				var once sync.Once
				resume := func() { once.Do(func() { close(releases.resume) }) }
				t.Cleanup(resume)
				reset := make(chan error, 1)
				go func() { reset <- f.bus.ResetInMemoryState() }()
				select {
				case claim := <-releases.entered:
					if claim != f.claims[0] {
						t.Fatalf("reset substituted exact claim: %+v", claim)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("reset did not enter selected-store Release")
				}
				retired := make(chan error, 1)
				group := &b10RetirementValidation{PublicationGroup: f.group, validated: make(chan struct{})}
				go func() {
					ctx, cancel := context.WithCancel(f.ctx)
					cancel()
					err := f.bus.FinalizeFanOutPublications(ctx, group, values)
					retired <- errors.Join(err, f.group.Close(context.Background()))
				}()
				awaitGroupProof(t, group.validated)
				select {
				case err := <-retired:
					t.Fatalf("group retirement escaped an admitted proxy release: %v", err)
				case <-time.After(25 * time.Millisecond):
				}
				resume()
				resetErr := receiveGroupProof(t, reset)
				if (releases.fault == nil && resetErr != nil) || (releases.fault != nil && !errors.Is(resetErr, releases.fault)) {
					t.Fatalf("reset lost/changed the real release outcome: %v", resetErr)
				}
				if errors.Is(resetErr, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("retirement raced backend release: %v", resetErr)
				}
				if err := receiveGroupProof(t, retired); !errors.Is(err, context.Canceled) || errors.Is(err, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("retirement changed exact dynamic failure: %v", err)
				}
				if releases.calls.Load() != 1 {
					t.Fatalf("reset release count=%d, want exact once", releases.calls.Load())
				}
				f.unchanged(t, before)
				if err := f.bus.ResetInMemoryState(); err != nil {
					t.Fatal(err)
				}
				work, err := f.store().ClaimEvent(f.ctx, f.events[0].ID(), pipelineobligation.PurposeRecovery)
				if err != nil || work.Claim.EventID() != f.events[0].ID() {
					t.Fatalf("retirement lost durable recovery: work=%+v err=%v", work, err)
				}
				if err := f.store().Release(f.ctx, f.claims[0]); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("retired exact claim regained authority: %v", err)
				}
				if err := f.store().Release(f.ctx, work.Claim); err != nil {
					t.Fatalf("old claim cleanup consumed successor: %v", err)
				}
			})
		}
	}
}
