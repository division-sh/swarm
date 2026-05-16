package destructivereset

import (
	"context"
)

type InventoryPlanner struct {
	Reader              InventoryReader
	DownstreamContracts []DownstreamContract
	ResetSeams          []ResetSeam
}

type CompositeInventoryReader struct {
	Reader     InventoryReader
	Containers ManagedContainerInventoryReader
}

func (r CompositeInventoryReader) ReadResetInventory(ctx context.Context) (Inventory, error) {
	if r.Reader == nil {
		return Inventory{}, ErrPlannerNotConfigured
	}
	inventory, err := r.Reader.ReadResetInventory(ctx)
	if err != nil {
		return Inventory{}, err
	}
	if r.Containers == nil {
		return inventory, nil
	}
	containers, err := r.Containers.ManagedResetContainerInventory(ctx)
	if err != nil {
		return Inventory{}, err
	}
	inventory.EntityContainers = append([]ContainerRef(nil), containers...)
	return inventory, nil
}

func (p InventoryPlanner) BuildPlan(ctx context.Context, _ Request) (Plan, error) {
	if p.Reader == nil {
		return Plan{}, ErrPlannerNotConfigured
	}
	inventory, err := p.Reader.ReadResetInventory(ctx)
	if err != nil {
		return Plan{}, err
	}
	preserved := mergePreservedResources(inventory.Preserved)
	contracts := p.DownstreamContracts
	if len(contracts) == 0 {
		contracts = DefaultDownstreamContracts()
	}
	seams := p.ResetSeams
	if len(seams) == 0 {
		seams = DefaultResetSeams()
	}
	return Plan{
		ActiveRuns:          append([]RunRef(nil), inventory.ActiveRuns...),
		CleanupRuns:         append([]RunRef(nil), inventory.CleanupRuns...),
		CleanupRunSetKnown:  inventory.CleanupRunSetKnown,
		ActiveDeliveries:    append([]DeliveryRef(nil), inventory.ActiveDeliveries...),
		RunScopedTables:     append([]TableRef(nil), inventory.RunScopedTables...),
		EntityContainers:    append([]ContainerRef(nil), inventory.EntityContainers...),
		Preserved:           copyPreservedResources(preserved),
		DownstreamContracts: append([]DownstreamContract(nil), contracts...),
		ResetSeams:          append([]ResetSeam(nil), seams...),
	}, nil
}

func copyPreservedResources(p PreservedResources) PreservedResources {
	p.SystemContainers = append([]string(nil), p.SystemContainers...)
	return p
}

func mergePreservedResources(p PreservedResources) PreservedResources {
	defaults := DefaultPreservedResources()
	if len(p.SystemContainers) == 0 {
		p.SystemContainers = defaults.SystemContainers
	}
	if p.OperatorManagedBoundary == "" {
		p.OperatorManagedBoundary = defaults.OperatorManagedBoundary
	}
	p.SchemaMigrations = p.SchemaMigrations || defaults.SchemaMigrations
	p.AuthTokens = p.AuthTokens || defaults.AuthTokens
	p.BundleContracts = p.BundleContracts || defaults.BundleContracts
	return copyPreservedResources(p)
}

func copyResult(result Result) Result {
	result.Plan = copyPlan(result.Plan)
	return result
}

func copyPlan(plan Plan) Plan {
	plan.ActiveRuns = append([]RunRef(nil), plan.ActiveRuns...)
	plan.CleanupRuns = append([]RunRef(nil), plan.CleanupRuns...)
	plan.ActiveDeliveries = append([]DeliveryRef(nil), plan.ActiveDeliveries...)
	plan.RunScopedTables = append([]TableRef(nil), plan.RunScopedTables...)
	plan.EntityContainers = append([]ContainerRef(nil), plan.EntityContainers...)
	plan.Preserved = copyPreservedResources(plan.Preserved)
	plan.DownstreamContracts = append([]DownstreamContract(nil), plan.DownstreamContracts...)
	plan.ResetSeams = append([]ResetSeam(nil), plan.ResetSeams...)
	return plan
}
