package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

// WorkflowMutationCommitObservationForTest is an opt-in scheduling observation
// after domain writes but before attempt finalization/native COMMIT. Its owner
// slot is dormant in production; neither SQL nor commit authority is exposed.
type WorkflowMutationCommitObservationForTest struct {
	original startupownership.MutationAuthority
	claim    deliverylifecycle.Claim
	mu       sync.Mutex
	entered  chan struct{}
	release  chan struct{}
	joined   chan struct{}
	started  bool
	closed   bool
	wait     *postgresbackend.MutationOrderWaitForTest
}

func (s *PipelinePostgresOwner) HoldWorkflowMutationCommitForTest(ctx context.Context, claim deliverylifecycle.Claim) (*WorkflowMutationCommitObservationForTest, error) {
	snapshot, err := s.Snapshot(ctx, claim.DeliveryID())
	if err != nil {
		return nil, err
	}
	return holdWorkflowMutationCommitForTest(ctx, &s.workflowCommitObservation, claim, snapshot)
}

func (s *PipelineSQLiteOwner) HoldWorkflowMutationCommitForTest(ctx context.Context, claim deliverylifecycle.Claim) (*WorkflowMutationCommitObservationForTest, error) {
	snapshot, err := s.Snapshot(ctx, claim.DeliveryID())
	if err != nil {
		return nil, err
	}
	return holdWorkflowMutationCommitForTest(ctx, &s.workflowCommitObservation, claim, snapshot)
}

func holdWorkflowMutationCommitForTest(ctx context.Context, slot *atomic.Pointer[WorkflowMutationCommitObservationForTest], claim deliverylifecycle.Claim, snapshot deliverylifecycle.Snapshot) (*WorkflowMutationCommitObservationForTest, error) {
	if err := claim.Validate(); err != nil {
		return nil, err
	}
	inbound, found := deliverylifecycle.ClaimFromContext(ctx)
	if !found || inbound != claim || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != claim.Version() ||
		snapshot.DeliveryID != claim.DeliveryID() || snapshot.RunID != claim.RunID() || snapshot.SubscriberID != claim.SubscriberID() {
		return nil, errors.New("workflow observation requires the original owner's exact active claim")
	}
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return nil, err
	}
	grant := original.GrantEvidence()
	if !original.HasRuntimeGrant() || snapshot.Authority.ExecutionID() != grant.GrantID || snapshot.Authority.Generation() != grant.RuntimeGeneration ||
		snapshot.Authority.SourceArtifact().BundleHash() != grant.BundleHash {
		return nil, errors.New("workflow observation claim and original generation differ")
	}
	probe := &WorkflowMutationCommitObservationForTest{original: original, claim: claim,
		entered: make(chan struct{}), release: make(chan struct{}), joined: make(chan struct{})}
	if !slot.CompareAndSwap(nil, probe) {
		return nil, errors.New("original workflow owner already has a commit observation")
	}
	return probe, nil
}

func exactWorkflowMutationObservationForTest(ctx context.Context, store eventCommitTxStore, command pipeline.WorkflowEngineMutationCommand) *WorkflowMutationCommitObservationForTest {
	var probe *WorkflowMutationCommitObservationForTest
	switch store := store.(type) {
	case *PipelinePostgresOwner:
		probe = store.workflowCommitObservation.Load()
	case *PipelineSQLiteOwner:
		probe = store.workflowCommitObservation.Load()
	}
	if probe == nil || command.DeliverySuccess == nil || command.DeliverySuccess.Claim != probe.claim {
		return nil
	}
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil || original.ProcessAuthority() != probe.original.ProcessAuthority() {
		return nil
	}
	actual, actualErr := canonicaljson.Bytes(original.GrantEvidence())
	want, wantErr := canonicaljson.Bytes(probe.original.GrantEvidence())
	if actualErr != nil || wantErr != nil || string(actual) != string(want) {
		return nil
	}
	return probe
}

func captureWorkflowMutationDomainForTest(ctx context.Context, tx *sql.Tx, store eventCommitTxStore, command pipeline.WorkflowEngineMutationCommand) error {
	probe := exactWorkflowMutationObservationForTest(ctx, store, command)
	owner, postgres := store.(*PipelinePostgresOwner)
	if probe == nil || !postgres {
		return nil
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.closed {
		return nil
	}
	if probe.started || probe.wait != nil {
		return errors.New("original workflow observation cannot capture another attempt")
	}
	wait, err := owner.backend.CaptureMutationOrderWaitForTest(ctx, tx)
	if err == nil {
		probe.wait = wait
	}
	return err
}

func observeWorkflowMutationDomainForTest(ctx context.Context, store eventCommitTxStore, command pipeline.WorkflowEngineMutationCommand) error {
	probe := exactWorkflowMutationObservationForTest(ctx, store, command)
	if probe == nil {
		return nil
	}
	probe.mu.Lock()
	if probe.closed || probe.started {
		probe.mu.Unlock()
		return nil
	}
	probe.started = true
	close(probe.entered)
	probe.mu.Unlock()
	defer close(probe.joined)
	defer probe.wait.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-probe.release:
		return ctx.Err()
	}
}

func (p *WorkflowMutationCommitObservationForTest) Entered() <-chan struct{} { return p.entered }

func (p *WorkflowMutationCommitObservationForTest) SuccessorWaiting(ctx context.Context) (bool, error) {
	if p == nil {
		return false, errors.New("original workflow observation is missing")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.started {
		return false, errors.New("original workflow observation is not held")
	}
	return p.wait.SuccessorWaiting(ctx)
}

func (p *WorkflowMutationCommitObservationForTest) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.release)
	}
	p.wait.Close()
	started := p.started
	p.mu.Unlock()
	if started {
		<-p.joined
	}
}
