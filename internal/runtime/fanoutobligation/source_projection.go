package fanoutobligation

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func (r IntentRequest) ValidateSourceProjection() error {
	if r.Deployment != nil {
		return nil
	}
	if err := r.PlanRef.Validate(); err != nil {
		return err
	}
	semantics := r.Capsule.SourceProjection
	source := r.Capsule.SourcePlanRef
	if source.ElementRef != r.PlanRef.ElementRef || strings.TrimSpace(source.BundleHash) == "" || strings.TrimSpace(source.SemanticDigest) == "" {
		return fmt.Errorf("fan-out source witness requires its exact original plan reference")
	}
	if source.BundleHash == r.PlanRef.BundleHash && source != r.PlanRef {
		return fmt.Errorf("fan-out source and execution references contradict the same artifact")
	}
	if err := semantics.Validate(source); err != nil {
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
	return r.ValidateExecutionProjection(plan.Ref, plan.SemanticEvidence())
}

func (r IntentRequest) AdmitSelectedPlan(ref runtimecontracts.FanOutPlanRef, semantics runtimecontracts.FanOutPlanSemantics) error {
	if r.PlanRef.BundleHash == ref.BundleHash && r.PlanRef != ref {
		return fmt.Errorf("unchanged bundle contradicts the retained fan-out execution reference")
	}
	r.PlanRef = ref
	return r.ValidateExecutionProjection(ref, semantics)
}

func (r IntentRequest) ValidateExecutionProjection(ref runtimecontracts.FanOutPlanRef, semantics runtimecontracts.FanOutPlanSemantics) error {
	if err := r.ValidateSourceProjection(); err != nil {
		return err
	}
	if ref != r.PlanRef {
		return fmt.Errorf("fan-out selected execution reference disagrees with loaded plan")
	}
	if err := semantics.Validate(ref); err != nil {
		return err
	}
	if r.Cardinality > semantics.MaxItems || !runtimecontracts.StructuralCatalogTypeAssignable(r.Capsule.SourceProjection.ItemType, semantics.ItemType) {
		return fmt.Errorf("fan-out selected plan cannot accept the frozen source items or cardinality")
	}
	return nil
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
