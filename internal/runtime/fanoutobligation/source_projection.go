package fanoutobligation

import (
	"fmt"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func (r IntentRequest) ValidateSourceProjection() error {
	if r.Deployment != nil {
		return nil
	}
	semantics := r.Capsule.SourceProjection
	if err := semantics.Validate(r.PlanRef); err != nil {
		return fmt.Errorf("fan-out admitted source evidence: %w", err)
	}
	var path string
	switch r.Source.Kind {
	case SourceEventPayloadField:
		path = "payload." + r.Source.Field
	case SourceEntityField:
		path = "entity." + r.Source.Field
	default:
		return fmt.Errorf("handler fan-out requires a field source")
	}
	if semantics.ItemsFrom != path || r.Cardinality < 0 || r.Cardinality > semantics.MaxItems {
		return fmt.Errorf("fan-out source identity or cardinality contradicts admitted projection")
	}
	return nil
}

func (r IntentRequest) ValidateCompiledPlan(plan runtimecontracts.FanOutCompiledPlan) error {
	if err := r.ValidateSourceProjection(); err != nil {
		return err
	}
	if plan.Ref != r.PlanRef {
		return fmt.Errorf("fan-out admitted source disagrees with loaded plan")
	}
	return plan.SemanticEvidence().Validate(r.PlanRef)
}

func (r IntentRequest) ProjectSource(value any) ([]any, error) {
	if err := r.ValidateSourceProjection(); err != nil {
		return nil, err
	}
	items, err := r.Capsule.SourceProjection.ProjectSource(value)
	if err != nil {
		return nil, err
	}
	if len(items) != r.Cardinality {
		return nil, fmt.Errorf("fan-out immutable source cardinality = %d, want %d", len(items), r.Cardinality)
	}
	return items, nil
}
