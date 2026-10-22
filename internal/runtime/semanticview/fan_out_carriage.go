package semanticview

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

// FanOutPlanCarriage retains compiled interpretation, not execution authority.
// Native fork writers cannot substitute caller-authored references for it.
type FanOutPlanCarriage struct{ state *fanOutPlanCarriage }

type fanOutPlanCarriage struct {
	bundleHash string
	plans      map[contracts.FanOutElementRef]fanOutPlanEvidence
}

type fanOutPlanEvidence struct {
	ref       contracts.FanOutPlanRef
	semantics contracts.FanOutPlanSemantics
}

func CompileFanOutPlanCarriage(source Source) (FanOutPlanCarriage, error) {
	bundle, ok := Bundle(source)
	if !ok || bundle.SourceArtifact == nil || bundle.SourceArtifact.BundleHash() == "" {
		return FanOutPlanCarriage{}, fmt.Errorf("fan-out plan carriage requires an admitted source artifact")
	}
	state := &fanOutPlanCarriage{bundleHash: bundle.SourceArtifact.BundleHash(), plans: make(map[contracts.FanOutElementRef]fanOutPlanEvidence)}
	for _, plan := range source.FanOutPlans() {
		if err := plan.Ref.Validate(); err != nil {
			return FanOutPlanCarriage{}, err
		}
		semantics := plan.SemanticEvidence()
		if err := semantics.Validate(plan.Ref); err != nil {
			return FanOutPlanCarriage{}, err
		}
		if plan.Ref.BundleHash != state.bundleHash {
			return FanOutPlanCarriage{}, fmt.Errorf("fan-out plan carriage contradicts its source artifact")
		}
		if _, duplicate := state.plans[plan.Ref.ElementRef]; duplicate {
			return FanOutPlanCarriage{}, fmt.Errorf("fan-out plan carriage has duplicate declaration evidence")
		}
		state.plans[plan.Ref.ElementRef] = fanOutPlanEvidence{ref: plan.Ref, semantics: semantics}
	}
	return FanOutPlanCarriage{state: state}, nil
}

func (c FanOutPlanCarriage) IsZero() bool { return c.state == nil }

func (c FanOutPlanCarriage) RequireSource(bundleHash string) error {
	if c.state == nil || c.state.bundleHash == "" || c.state.bundleHash != bundleHash {
		return fmt.Errorf("fan-out plan carriage does not belong to the exact selected source artifact")
	}
	return nil
}

func (c FanOutPlanCarriage) Resolve(element contracts.FanOutElementRef) (contracts.FanOutPlanRef, contracts.FanOutPlanSemantics, error) {
	if c.state == nil {
		return contracts.FanOutPlanRef{}, contracts.FanOutPlanSemantics{}, fmt.Errorf("fan-out selected plan has no proof")
	}
	evidence, ok := c.state.plans[element]
	if !ok {
		return contracts.FanOutPlanRef{}, contracts.FanOutPlanSemantics{}, fmt.Errorf("selected source has no fan-out declaration proof")
	}
	return evidence.ref, evidence.semantics.Clone(), nil
}
