package startupownership

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

type GrantState string

const (
	GrantPrepared     GrantState = "prepared"
	GrantProbeSettled GrantState = "probe_settled"
	GrantAdmitted     GrantState = "admitted"
	GrantRetired      GrantState = "retired"
)

type GrantRequest struct {
	BundleHash        string
	BundleSource      string
	RuntimeInstanceID string
	RuntimeGeneration uint64
	SourceSetRevision string
}

func (r GrantRequest) Validate() error {
	if err := runtimebundleidentity.ValidateCanonicalHash(strings.TrimSpace(r.BundleHash)); err != nil {
		return fmt.Errorf("runtime generation grant bundle_hash is invalid: %w", err)
	}
	switch strings.TrimSpace(r.BundleSource) {
	case "persisted", "ephemeral":
	default:
		return errors.New("runtime generation grant bundle_source must be persisted or ephemeral")
	}
	if _, err := uuid.Parse(strings.TrimSpace(r.RuntimeInstanceID)); err != nil {
		return fmt.Errorf("runtime generation grant runtime_instance_id is invalid: %w", err)
	}
	if r.RuntimeGeneration == 0 {
		return errors.New("runtime generation grant generation must be positive")
	}
	if strings.TrimSpace(r.SourceSetRevision) == "" {
		return errors.New("runtime generation grant source_set_revision is required")
	}
	return nil
}

type GrantEvidence struct {
	GrantID            string     `json:"grant_id"`
	ProcessAuthorityID string     `json:"process_authority_id"`
	ProcessOwnerID     string     `json:"process_owner_id"`
	ProcessBootID      string     `json:"process_boot_id"`
	BundleHash         string     `json:"bundle_hash"`
	BundleSource       string     `json:"bundle_source"`
	RuntimeInstanceID  string     `json:"runtime_instance_id"`
	RuntimeGeneration  uint64     `json:"runtime_generation"`
	SourceSetRevision  string     `json:"source_set_revision"`
	StateVersion       uint64     `json:"state_version"`
	State              GrantState `json:"state"`
	ProbeSurfaceIDs    []string   `json:"probe_surface_ids,omitempty"`
}

func (e GrantEvidence) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(e.GrantID)); err != nil {
		return fmt.Errorf("runtime generation grant id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(e.ProcessAuthorityID)); err != nil {
		return fmt.Errorf("runtime generation grant process authority is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(e.ProcessBootID)); err != nil {
		return fmt.Errorf("runtime generation grant process boot is invalid: %w", err)
	}
	if strings.TrimSpace(e.ProcessOwnerID) == "" || e.StateVersion == 0 {
		return errors.New("runtime generation grant process evidence is incomplete")
	}
	if err := (GrantRequest{
		BundleHash: e.BundleHash, BundleSource: e.BundleSource,
		RuntimeInstanceID: e.RuntimeInstanceID, RuntimeGeneration: e.RuntimeGeneration,
		SourceSetRevision: e.SourceSetRevision,
	}).Validate(); err != nil {
		return err
	}
	switch e.State {
	case GrantPrepared, GrantProbeSettled, GrantAdmitted, GrantRetired:
		return nil
	default:
		return fmt.Errorf("runtime generation grant state %q is invalid", e.State)
	}
}

type GenerationGrant interface {
	runtimemanager.AgentLifecyclePersistence
	Evidence() (GrantEvidence, error)
	SourceSetPlan(context.Context) (runtimeagenttopology.SourceSetPlan, error)
	ProveCurrent(context.Context) error
	MarkProbesSettled(context.Context, []string) (GrantEvidence, error)
	AdmitExecution(context.Context) (GrantEvidence, error)
	Retire(context.Context) error
	Done() <-chan struct{}
}

type ProcessCapability interface {
	Evidence() (Authority, error)
	CurrentSourceSet(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error)
	IssueGenerationGrant(context.Context, GrantRequest) (GenerationGrant, error)
	InstallCompleteSourceSet(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	ReplaceSourceSet(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	RestoreSourceSet(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	RemoveBundleSource(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	ApplyBundleDeleteFinalMutation(context.Context, runtimebundledelete.FinalMutationRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error)
	ReplayBundleDeleteResult(context.Context, runtimebundledelete.FinalMutationRequest) (runtimebundledelete.Result, error)
	ApplyDestructiveResetCleanup(context.Context, runtimedestructivereset.CleanupRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error)
	ApplyDestructiveResetTopology(context.Context, runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error)
	Release(context.Context) error
	Done() <-chan struct{}
}

type processCapability struct {
	opMu     sync.Mutex
	mu       sync.Mutex
	session  RetainedSession
	grants   map[string]*generationGrant
	done     chan struct{}
	doneOnce sync.Once
}

type generationGrant struct {
	mu       sync.Mutex
	owner    *processCapability
	evidence GrantEvidence
	done     chan struct{}
	doneOnce sync.Once
}

func NewProcessCapability(session RetainedSession) (ProcessCapability, error) {
	if session == nil {
		return nil, errors.New("process startup/topology capability requires a retained selected-store session")
	}
	if _, err := session.Authority(); err != nil {
		return nil, err
	}
	p := &processCapability{session: session, grants: map[string]*generationGrant{}, done: make(chan struct{})}
	if err := session.InstallTerminalOwner(p); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *processCapability) Evidence() (Authority, error) {
	if err := p.requireLive(); err != nil {
		return Authority{}, err
	}
	return p.session.Authority()
}

func (p *processCapability) CurrentSourceSet(ctx context.Context) (runtimeagenttopology.SourceSetPlan, bool, error) {
	if p == nil {
		return runtimeagenttopology.SourceSetPlan{}, false, errors.New("process startup/topology capability is missing")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return runtimeagenttopology.SourceSetPlan{}, false, err
	}
	plan, exists, err := p.session.LoadSourceSet(ctx)
	if err != nil {
		p.retireOnPossessionFailure(err)
	}
	return plan, exists, err
}

func (p *processCapability) IssueGenerationGrant(ctx context.Context, req GrantRequest) (GenerationGrant, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return nil, err
	}
	authority, err := p.session.Authority()
	if err != nil {
		p.retire()
		return nil, err
	}
	if authority.RuntimeInstanceID != strings.TrimSpace(req.RuntimeInstanceID) {
		return nil, errors.New("runtime generation grant runtime instance differs from process authority")
	}
	plan, exists, err := p.session.LoadSourceSet(ctx)
	if err != nil {
		p.retireOnPossessionFailure(err)
		return nil, err
	}
	if !exists || plan.Revision != strings.TrimSpace(req.SourceSetRevision) {
		return nil, errors.New("runtime generation grant requires the current complete source set")
	}
	wantedSource := runtimeagenttopology.SourceCoordinate{
		BundleHash: req.BundleHash, BundleSource: req.BundleSource,
	}.Normalize()
	sourceCurrent := false
	for _, source := range plan.Sources {
		if source.Normalize().Key() == wantedSource.Key() {
			sourceCurrent = true
			break
		}
	}
	if !sourceCurrent {
		return nil, errors.New("runtime generation grant source is absent from the current complete source set")
	}
	evidence := GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: authority.AuthorityID,
		ProcessOwnerID: authority.OwnerID, ProcessBootID: authority.BootID,
		BundleHash: strings.TrimSpace(req.BundleHash), BundleSource: strings.TrimSpace(req.BundleSource),
		RuntimeInstanceID: strings.TrimSpace(req.RuntimeInstanceID), RuntimeGeneration: req.RuntimeGeneration,
		SourceSetRevision: strings.TrimSpace(req.SourceSetRevision), StateVersion: 1, State: GrantPrepared,
	}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	if err := p.session.RecordGenerationGrantTransition(ctx, nil, evidence); err != nil {
		p.retireOnPossessionFailure(err)
		return nil, err
	}
	g := &generationGrant{owner: p, evidence: evidence, done: make(chan struct{})}
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		return nil, errors.New("process startup/topology capability is terminal")
	default:
	}
	p.grants[evidence.GrantID] = g
	return g, nil
}

func (p *processCapability) InstallCompleteSourceSet(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	return p.commitSourceSet(ctx, runtimeagenttopology.OperationInstallCompleteSourceSet, req)
}

func (p *processCapability) ReplaceSourceSet(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	return p.commitSourceSet(ctx, runtimeagenttopology.OperationReplaceSourceSet, req)
}

func (p *processCapability) RestoreSourceSet(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	return p.commitSourceSet(ctx, runtimeagenttopology.OperationRestoreSourceSet, req)
}

func (p *processCapability) RemoveBundleSource(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	return p.commitSourceSet(ctx, runtimeagenttopology.OperationRemoveBundleSource, req)
}

func (p *processCapability) ApplyBundleDeleteFinalMutation(ctx context.Context, req runtimebundledelete.FinalMutationRequest, topology *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error) {
	if p == nil {
		return runtimebundledelete.FinalMutationResult{}, errors.New("process startup/topology capability is missing")
	}
	if topology != nil {
		topology.Operation = runtimeagenttopology.OperationRemoveBundleSource
		if err := topology.Validate(); err != nil {
			return runtimebundledelete.FinalMutationResult{}, err
		}
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return runtimebundledelete.FinalMutationResult{}, err
	}
	result, err := p.session.ApplyBundleDeleteFinalMutation(ctx, req, topology)
	if err != nil {
		p.retireOnPossessionFailure(err)
	}
	return result, err
}

func (p *processCapability) ReplayBundleDeleteResult(ctx context.Context, req runtimebundledelete.FinalMutationRequest) (runtimebundledelete.Result, error) {
	if p == nil {
		return runtimebundledelete.Result{}, errors.New("process startup/topology capability is missing")
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return runtimebundledelete.Result{}, err
	}
	result, err := p.session.ReplayBundleDeleteResult(ctx, req)
	if err != nil {
		p.retireOnPossessionFailure(err)
	}
	return result, err
}

func (p *processCapability) ApplyDestructiveResetCleanup(ctx context.Context, req runtimedestructivereset.CleanupRequest, topology *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error) {
	if p == nil {
		return runtimedestructivereset.CleanupResult{}, errors.New("process startup/topology capability is missing")
	}
	if topology != nil {
		topology.Operation = runtimeagenttopology.OperationApplyDestructiveResetTopology
		if err := topology.Validate(); err != nil {
			return runtimedestructivereset.CleanupResult{}, err
		}
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return runtimedestructivereset.CleanupResult{}, err
	}
	result, err := p.session.ApplyDestructiveResetCleanup(ctx, req, topology)
	if err != nil {
		p.retireOnPossessionFailure(err)
	}
	return result, err
}

func (p *processCapability) ApplyDestructiveResetTopology(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	return p.commitSourceSet(ctx, runtimeagenttopology.OperationApplyDestructiveResetTopology, req)
}

func (p *processCapability) commitSourceSet(ctx context.Context, operation runtimeagenttopology.SourceSetOperation, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	req.Operation = operation
	if err := req.Validate(); err != nil {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	result, err := p.session.CommitSourceSet(ctx, req)
	if err != nil {
		p.retireOnPossessionFailure(err)
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	return result, nil
}

func (p *processCapability) Release(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if p.session == nil {
		p.retire()
		return nil
	}
	for _, grant := range p.snapshotGrants() {
		if err := grant.retireWithSession(ctx); err != nil {
			p.retireOnPossessionFailure(err)
			return err
		}
	}
	err := p.session.Release(ctx)
	p.retire()
	return err
}

func (p *processCapability) Done() <-chan struct{} {
	if p == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return p.done
}

func (p *processCapability) proveCurrent(ctx context.Context) error {
	if err := p.requireLive(); err != nil {
		return err
	}
	if err := p.session.ProveCurrent(ctx); err != nil {
		p.retire()
		return fmt.Errorf("prove current process startup/topology capability: %w", err)
	}
	return p.requireLive()
}

func (p *processCapability) requireLive() error {
	if p == nil {
		return errors.New("process startup/topology capability is missing")
	}
	select {
	case <-p.done:
		return errors.New("process startup/topology capability is terminal")
	default:
		return nil
	}
}

func (p *processCapability) retireOnPossessionFailure(err error) {
	if err == nil {
		return
	}
	// Closed selected-store operations own semantic failures without implying
	// session loss. A failed proof or terminal callback performs retirement.
	if proveErr := p.session.ProveCurrent(context.Background()); proveErr != nil {
		p.retire()
	}
}

func (p *processCapability) snapshotGrants() []*generationGrant {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*generationGrant, 0, len(p.grants))
	for _, grant := range p.grants {
		out = append(out, grant)
	}
	return out
}

func (p *processCapability) retire() {
	if p == nil {
		return
	}
	p.doneOnce.Do(func() {
		close(p.done)
		p.mu.Lock()
		grants := make([]*generationGrant, 0, len(p.grants))
		for _, grant := range p.grants {
			grants = append(grants, grant)
		}
		p.grants = map[string]*generationGrant{}
		p.mu.Unlock()
		for _, grant := range grants {
			grant.retireLocal()
		}
	})
}

func (p *processCapability) SelectedStoreSessionTerminal() {
	p.retire()
}

func (g *generationGrant) Evidence() (GrantEvidence, error) {
	if g == nil {
		return GrantEvidence{}, errors.New("runtime generation grant is missing")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.evidence.State == GrantRetired {
		return GrantEvidence{}, errors.New("runtime generation grant is retired")
	}
	return g.evidence, nil
}

func (g *generationGrant) SourceSetPlan(ctx context.Context) (runtimeagenttopology.SourceSetPlan, error) {
	if g == nil || g.owner == nil {
		return runtimeagenttopology.SourceSetPlan{}, errors.New("runtime generation grant is missing its process owner")
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return runtimeagenttopology.SourceSetPlan{}, err
	}
	g.mu.Lock()
	evidence := g.evidence
	g.mu.Unlock()
	if evidence.State == GrantRetired {
		return runtimeagenttopology.SourceSetPlan{}, errors.New("runtime generation grant is retired")
	}
	return g.requireCurrentSourceSetLocked(ctx, evidence)
}

// requireCurrentSourceSetLocked is called while the process operation lock is
// held, so the source-set head cannot change between this proof and a mutation.
func (g *generationGrant) requireCurrentSourceSetLocked(ctx context.Context, evidence GrantEvidence) (runtimeagenttopology.SourceSetPlan, error) {
	plan, exists, err := g.owner.session.LoadSourceSet(ctx)
	if err != nil {
		g.owner.retireOnPossessionFailure(err)
		return runtimeagenttopology.SourceSetPlan{}, err
	}
	if !exists || plan.Revision != evidence.SourceSetRevision {
		return runtimeagenttopology.SourceSetPlan{}, errors.New("runtime generation grant source-set revision is not current")
	}
	return plan, nil
}

func (g *generationGrant) ProveCurrent(ctx context.Context) error {
	if g == nil || g.owner == nil {
		return errors.New("runtime generation grant is missing its process owner")
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return err
	}
	g.mu.Lock()
	evidence := g.evidence
	g.mu.Unlock()
	if evidence.State == GrantRetired {
		return errors.New("runtime generation grant is retired")
	}
	_, err := g.requireCurrentSourceSetLocked(ctx, evidence)
	return err
}

func (g *generationGrant) MarkProbesSettled(ctx context.Context, surfaceIDs []string) (GrantEvidence, error) {
	return g.transition(ctx, GrantPrepared, GrantProbeSettled, surfaceIDs)
}

func (g *generationGrant) AdmitExecution(ctx context.Context) (GrantEvidence, error) {
	return g.transition(ctx, GrantProbeSettled, GrantAdmitted, nil)
}

func (g *generationGrant) transition(ctx context.Context, from, to GrantState, probeSurfaceIDs []string) (GrantEvidence, error) {
	if g == nil || g.owner == nil {
		return GrantEvidence{}, errors.New("runtime generation grant is missing its process owner")
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return GrantEvidence{}, err
	}
	g.mu.Lock()
	evidence := g.evidence
	g.mu.Unlock()
	if evidence.State == GrantRetired {
		return GrantEvidence{}, errors.New("runtime generation grant is retired")
	}
	if _, err := g.requireCurrentSourceSetLocked(ctx, evidence); err != nil {
		return GrantEvidence{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.evidence.State != from {
		return GrantEvidence{}, fmt.Errorf("runtime generation grant transition %s -> %s rejected from %s", from, to, g.evidence.State)
	}
	previous := g.evidence
	next := previous
	next.State = to
	next.StateVersion++
	if to == GrantProbeSettled {
		next.ProbeSurfaceIDs = normalizeIDs(probeSurfaceIDs)
	}
	if err := next.Validate(); err != nil {
		return GrantEvidence{}, err
	}
	if err := g.owner.session.RecordGenerationGrantTransition(ctx, &previous, next); err != nil {
		g.owner.retireOnPossessionFailure(err)
		return GrantEvidence{}, err
	}
	g.evidence = next
	return next, nil
}

func (g *generationGrant) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	if g == nil || g.owner == nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, errors.New("runtime generation grant is missing its process owner")
	}
	if err := req.Topology.Validate(); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.owner.proveCurrent(ctx); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	g.mu.Lock()
	evidence := g.evidence
	g.mu.Unlock()
	if evidence.State == GrantRetired {
		return runtimemanager.AgentLifecycleTransitionResult{}, errors.New("runtime generation grant is retired")
	}
	if _, err := g.requireCurrentSourceSetLocked(ctx, evidence); err != nil {
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	if req.Topology.Authority.Kind == runtimeagenttopology.AuthorityStaticDeclarationPlan {
		static := req.Topology.Authority.Static
		if static.SourceSetRevision != evidence.SourceSetRevision || static.BundleHash != evidence.BundleHash || static.BundleSource != evidence.BundleSource {
			return runtimemanager.AgentLifecycleTransitionResult{}, errors.New("static lifecycle topology authority differs from runtime generation grant")
		}
	}
	mutationCtx := runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash))
	result, err := g.owner.session.CommitAgentLifecycleTransition(mutationCtx, req)
	if err != nil {
		g.owner.retireOnPossessionFailure(err)
		return runtimemanager.AgentLifecycleTransitionResult{}, err
	}
	return result, nil
}

func (g *generationGrant) Retire(ctx context.Context) error {
	if g == nil || g.owner == nil {
		return nil
	}
	g.owner.opMu.Lock()
	defer g.owner.opMu.Unlock()
	if err := g.retireWithSession(ctx); err != nil {
		g.owner.retireOnPossessionFailure(err)
		return err
	}
	return nil
}

func (g *generationGrant) retireWithSession(ctx context.Context) error {
	g.mu.Lock()
	if g.evidence.State == GrantRetired {
		g.mu.Unlock()
		return nil
	}
	previous := g.evidence
	next := previous
	next.State = GrantRetired
	next.StateVersion++
	if err := g.owner.session.RecordGenerationGrantTransition(ctx, &previous, next); err != nil {
		g.mu.Unlock()
		return err
	}
	g.evidence = next
	g.mu.Unlock()
	g.retireLocal()
	g.owner.mu.Lock()
	delete(g.owner.grants, next.GrantID)
	g.owner.mu.Unlock()
	return nil
}

func (g *generationGrant) retireLocal() {
	g.doneOnce.Do(func() {
		g.mu.Lock()
		g.evidence.State = GrantRetired
		g.mu.Unlock()
		close(g.done)
	})
}

func (g *generationGrant) Done() <-chan struct{} {
	if g == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return g.done
}

func normalizeIDs(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
