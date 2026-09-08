package agenttopology

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

// SelectedDeclarationPlan is the immutable target declaration census for one
// prepared selected execution. It has no live source-set membership meaning.
type SelectedDeclarationPlan struct {
	BundleHash string         `json:"bundle_hash"`
	Revision   string         `json:"revision"`
	Agents     []DesiredAgent `json:"agents"`
}

func NewSelectedDeclarationPlan(bundleHash string, agents []DesiredAgent) (SelectedDeclarationPlan, error) {
	p := SelectedDeclarationPlan{BundleHash: bundleHash, Agents: append([]DesiredAgent{}, agents...)}
	sort.Slice(p.Agents, func(i, j int) bool { return agentidentity.LessPlan(p.Agents[i].Identity, p.Agents[j].Identity) })
	var err error
	p.Revision, err = p.fingerprint()
	if err != nil {
		return SelectedDeclarationPlan{}, err
	}
	return p, p.Validate()
}

func (p SelectedDeclarationPlan) fingerprint() (string, error) {
	return canonicaljson.Hash(struct {
		BundleHash string         `json:"bundle_hash"`
		Agents     []DesiredAgent `json:"agents"`
	}{p.BundleHash, p.Agents})
}

func (p SelectedDeclarationPlan) Validate() error {
	if err := bundleidentity.ValidateCanonicalHash(p.BundleHash); err != nil {
		return err
	}
	if p.Agents == nil {
		return errors.New("selected declaration plan requires an explicit census")
	}
	for i, desired := range p.Agents {
		if err := desired.Validate(); err != nil {
			return err
		}
		if desired.Identity != desired.Identity.Normalize() || desired.Source.BundleHash != p.BundleHash || strings.TrimSpace(desired.ConfigRevision) != desired.ConfigRevision {
			return errors.New("selected declaration contains a noncanonical or different-source agent")
		}
		if i > 0 && !agentidentity.LessPlan(p.Agents[i-1].Identity, desired.Identity) {
			return errors.New("selected declaration census is duplicated or out of order")
		}
	}
	want, err := p.fingerprint()
	if err != nil {
		return err
	}
	if p.Revision != want {
		return errors.New("selected declaration fingerprint differs from canonical plan")
	}
	return nil
}

type SelectedForkDeclarationPlan struct {
	RunID           string `json:"run_id"`
	BundleHash      string `json:"bundle_hash"`
	PlanFingerprint string `json:"plan_fingerprint"`
}

func SelectedDeclarationAdmission(runID string, plan SelectedDeclarationPlan) (Admission, error) {
	if err := plan.Validate(); err != nil {
		return Admission{}, err
	}
	a := Admission{
		Authority: Authority{Kind: AuthoritySelectedForkDeclarationPlan, Selected: &SelectedForkDeclarationPlan{
			RunID: runID, BundleHash: plan.BundleHash, PlanFingerprint: plan.Revision,
		}},
		Lifetime: LifetimeDurableManaged,
	}
	return a, a.Validate()
}

func (p SelectedForkDeclarationPlan) Validate() error {
	id, err := uuid.Parse(p.RunID)
	if err != nil || id == uuid.Nil || id.String() != p.RunID {
		return errors.New("selected declaration requires a canonical nonzero run UUID")
	}
	if err := bundleidentity.ValidateCanonicalHash(p.BundleHash); err != nil {
		return err
	}
	if err := bundleidentity.ValidateCanonicalHash("bundle-v2:" + p.PlanFingerprint); err != nil {
		return fmt.Errorf("selected declaration fingerprint: %w", err)
	}
	return nil
}
