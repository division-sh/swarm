package startupownership

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

// SelectedForkGrantBinding is durable evidence, not a capability. The retained
// session checks its exact binding and execution again at each authority use.
type SelectedForkGrantBinding struct {
	BindingID                  string `json:"binding_id"`
	ForkRunID                  string `json:"fork_run_id"`
	ExecutionID                string `json:"execution_id"`
	ExecutionGeneration        uint64 `json:"execution_generation"`
	FenceGeneration            uint64 `json:"fence_generation"`
	ExecutionOwner             string `json:"execution_owner"`
	AdmissionFingerprint       string `json:"admission_fingerprint"`
	ContainerPlanFingerprint   string `json:"container_plan_fingerprint"`
	ActorCensusFingerprint     string `json:"actor_census_fingerprint"`
	EffectiveConfigFingerprint string `json:"effective_config_fingerprint"`
	DeclarationPlanFingerprint string `json:"declaration_plan_fingerprint"`
	PreparationFingerprint     string `json:"preparation_fingerprint"`
}

func (b SelectedForkGrantBinding) Validate() error {
	for _, id := range []string{b.BindingID, b.ForkRunID, b.ExecutionID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return errors.New("selected-fork grant requires canonical nonzero binding, run and execution UUIDs")
		}
	}
	if b.ExecutionGeneration == 0 || b.FenceGeneration == 0 {
		return errors.New("selected-fork grant requires positive execution and fence generations")
	}
	if b.ExecutionOwner == "" || strings.TrimSpace(b.ExecutionOwner) != b.ExecutionOwner {
		return errors.New("selected-fork grant requires an exact execution owner")
	}
	for _, fingerprint := range []string{b.AdmissionFingerprint, b.ContainerPlanFingerprint, b.ActorCensusFingerprint, b.EffectiveConfigFingerprint, b.DeclarationPlanFingerprint, b.PreparationFingerprint} {
		hexValue, found := strings.CutPrefix(fingerprint, "sha256:")
		decoded, err := hex.DecodeString(hexValue)
		if !found || err != nil || len(decoded) != 32 || strings.ToLower(hexValue) != hexValue {
			return errors.New("selected-fork grant requires canonical execution fingerprints")
		}
	}
	return nil
}

type SelectedForkGrantRequest struct {
	BundleHash        string
	RuntimeInstanceID string
	Binding           SelectedForkGrantBinding
}

func (r SelectedForkGrantRequest) Validate() error {
	if err := r.Binding.Validate(); err != nil {
		return err
	}
	if err := bundleidentity.ValidateCanonicalHash(r.BundleHash); err != nil {
		return err
	}
	id, err := uuid.Parse(r.RuntimeInstanceID)
	if err != nil || id == uuid.Nil || id.String() != r.RuntimeInstanceID {
		return errors.New("selected-fork grant runtime instance must be a canonical nonzero UUID")
	}
	return nil
}

func (p *processCapability) IssueSelectedForkGenerationGrant(ctx context.Context, req SelectedForkGrantRequest) (GenerationGrant, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p.opMu.Lock()
	defer p.opMu.Unlock()
	if err := p.proveCurrent(ctx); err != nil {
		return nil, err
	}
	process, err := p.session.Authority()
	if err != nil {
		return nil, err
	}
	if process.RuntimeInstanceID != req.RuntimeInstanceID {
		return nil, errors.New("selected-fork grant runtime instance differs from process authority")
	}
	binding := req.Binding
	evidence := GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: process.AuthorityID,
		ProcessOwnerID: process.OwnerID, ProcessBootID: process.BootID,
		BundleHash: req.BundleHash, RuntimeInstanceID: req.RuntimeInstanceID,
		RuntimeGeneration: binding.ExecutionGeneration, StateVersion: 1, State: GrantPrepared,
		SelectedFork: &binding,
	}
	if err := p.session.RecordGenerationGrantTransition(ctx, nil, evidence); err != nil {
		err = p.retireOnPossessionFailure(err)
		return nil, fmt.Errorf("record selected-fork generation grant: %w", err)
	}
	g := &generationGrant{owner: p, evidence: evidence, done: make(chan struct{})}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.requireLive(); err != nil {
		return nil, err
	}
	p.grants[evidence.GrantID] = g
	return g, nil
}

func (e GrantEvidence) clone() GrantEvidence {
	if e.SelectedFork != nil {
		binding := *e.SelectedFork
		e.SelectedFork = &binding
	}
	e.ProbeSurfaceIDs = append([]string(nil), e.ProbeSurfaceIDs...)
	return e
}

func (g *generationGrant) requireExecutionAuthorityLocked(ctx context.Context, evidence GrantEvidence) error {
	if evidence.SelectedFork != nil {
		err := g.owner.session.ProveSelectedForkGenerationGrant(ctx, evidence)
		if err != nil {
			err = g.owner.retireOnPossessionFailure(err)
		}
		return errors.Join(err, g.owner.requireLive())
	}
	_, err := g.requireCurrentSourceSetLocked(ctx, evidence)
	return err
}
