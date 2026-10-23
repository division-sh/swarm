package runforkexecution

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func selectedForkParentRuntimeContext(t *testing.T, ctx context.Context, selected startupownership.Store, source correlation.SourceArtifactFact) (context.Context, string) {
	t.Helper()
	process := selectedContractTestProcessCapability(t, ctx, selected)
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: source.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	processEvidence, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: source.BundleHash(), RuntimeInstanceID: processEvidence.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := grant.Retire(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	})
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ctx = correlation.WithSourceArtifactFact(ctx, source)
	ctx = correlation.WithRuntimeInstanceID(ctx, evidence.RuntimeInstanceID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash))
	ctx, err = startupownership.WithRuntimeMutationAuthority(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		original, err := startupownership.CaptureMutationAuthority(context.WithoutCancel(ctx))
		if err != nil || original.GrantEvidence().GrantID != evidence.GrantID {
			t.Errorf("selected child changed its parent original: %v", err)
		}
		if err := grant.ProveCurrent(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("selected child retired its parent grant: %v", err)
		}
	})
	return ctx, evidence.GrantID
}

type selectedChildContextProofCapability struct {
	startupownership.ProcessCapability
	t       *testing.T
	parent  context.Context
	foreign context.Context
	issued  []*selectedChildContextProofGrant
}

type selectedChildContextProofGrant struct {
	startupownership.GenerationGrant
	proof         *selectedChildContextProofCapability
	ctx           context.Context
	stale         context.Context
	gatewayChecks atomic.Int64
}

type selectedChildContextProofKey struct{}

func selectedForkChildContextProof(t *testing.T, parent context.Context, selected startupownership.Store, source correlation.SourceArtifactFact) *selectedChildContextProofCapability {
	t.Helper()
	foreign, _ := selectedForkParentRuntimeContext(t, runForkTestContext(t), storetest.StartSQLiteRuntimeStore(t), source)
	return &selectedChildContextProofCapability{
		ProcessCapability: selectedContractTestProcessCapability(t, parent, selected),
		t:                 t, parent: parent, foreign: foreign,
	}
}

func (p *selectedChildContextProofCapability) IssueSelectedForkGenerationGrant(ctx context.Context, req startupownership.SelectedForkGrantRequest) (startupownership.GenerationGrant, error) {
	grant, err := p.ProcessCapability.IssueSelectedForkGenerationGrant(ctx, req)
	if err != nil {
		return nil, err
	}
	proof := &selectedChildContextProofGrant{GenerationGrant: grant, proof: p}
	p.issued = append(p.issued, proof)
	if _, err := proof.BindSelectedForkMutationContext(ctx); err != nil {
		return nil, err
	}
	return grant, nil
}

func (g *selectedChildContextProofGrant) BindSelectedForkMutationContext(ctx context.Context) (context.Context, error) {
	t := g.proof.t
	owner := g.GenerationGrant.(startupownership.SelectedForkMutationContextOwner)
	if _, err := startupownership.WithRuntimeMutationAuthority(g.proof.parent, g.GenerationGrant); err == nil {
		t.Fatal("ordinary binding replaced the parent's original generation")
	}
	authority, _ := effects.AuthorityFromContext(ctx)
	admission, _ := managedexecution.FromContext(ctx)
	missing := managedexecution.WithAdmission(effects.WithAuthority(context.Background(), authority), admission)
	foreign := managedexecution.WithAdmission(effects.WithAuthority(g.proof.foreign, authority), admission)
	wrongRun := authority
	wrongRun.SelectedFork.ForkRunID = uuid.NewString()
	wrongGeneration := authority
	wrongGeneration.SelectedFork.Generation++
	refused, err := startupownership.WithRuntimeMutationAuthority(g.proof.parent, g.GenerationGrant)
	if err == nil {
		t.Fatal("refused-owner control unexpectedly bound")
	}
	for name, candidate := range map[string]context.Context{
		"missing": missing, "foreign_process": foreign, "refused_original": refused,
		"cross_fork": effects.WithAuthority(ctx, wrongRun), "wrong_generation": effects.WithAuthority(ctx, wrongGeneration),
	} {
		if _, err := owner.BindSelectedForkMutationContext(candidate); err == nil {
			t.Fatalf("selected child context admitted %s", name)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := owner.BindSelectedForkMutationContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("selected child lost incoming cancellation: %v", err)
	}
	bounded, stop := context.WithDeadline(context.WithValue(ctx, selectedChildContextProofKey{}, g), time.Now().Add(time.Minute))
	defer stop()
	child, err := owner.BindSelectedForkMutationContext(bounded)
	if err != nil {
		return nil, err
	}
	wantDeadline, _ := bounded.Deadline()
	gotDeadline, ok := child.Deadline()
	if !ok || gotDeadline != wantDeadline || child.Done() != bounded.Done() || child.Value(selectedChildContextProofKey{}) != g {
		t.Fatal("selected child changed cancellation, deadline or ordinary values")
	}
	reused, err := owner.BindSelectedForkMutationContext(child)
	if err != nil {
		return nil, err
	}
	original, err := startupownership.CaptureMutationAuthority(reused)
	evidence, evidenceErr := g.GenerationGrant.Evidence()
	if err != nil || evidenceErr != nil || original.GrantEvidence().GrantID != evidence.GrantID {
		t.Fatal("same-child reuse changed the genuine issuing owner")
	}
	g.ctx, err = owner.BindSelectedForkMutationContext(context.WithValue(ctx, selectedChildContextProofKey{}, g))
	if err == nil && g.stale == nil {
		g.stale, err = startupownership.FreezeMutationAuthority(g.ctx)
	}
	return g.ctx, err
}

func (p *selectedChildContextProofCapability) requireReleasedChildren(t *testing.T) {
	t.Helper()
	select {
	case <-p.ProcessCapability.Done():
	default:
		t.Error("released-owner control ran before actual process release")
		return
	}
	for _, grant := range p.issued {
		owner := grant.GenerationGrant.(startupownership.SelectedForkMutationContextOwner)
		// Exclude cancellation as the reason this negative control refuses.
		if _, err := owner.BindSelectedForkMutationContext(context.WithoutCancel(grant.ctx)); err == nil {
			t.Error("released process regained a selected child operation")
		}
	}
}
