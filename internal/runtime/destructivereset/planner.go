package destructivereset

import (
	"context"
)

type InventoryPlanner struct {
	Reader              InventoryReader
	DownstreamContracts []DownstreamContract
	ResetSeams          []ResetSeam
}

func (p InventoryPlanner) BuildPlan(ctx context.Context, _ Request) (Plan, error) {
	if p.Reader == nil {
		return Plan{}, ErrPlannerNotConfigured
	}
	inventory, err := p.Reader.ReadResetInventory(ctx)
	if err != nil {
		return Plan{}, err
	}
	preserved := inventory.Preserved
	if !hasPreservedResources(preserved) {
		preserved = DefaultPreservedResources()
	}
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
		ActiveDeliveries:    append([]DeliveryRef(nil), inventory.ActiveDeliveries...),
		RunScopedTables:     append([]TableRef(nil), inventory.RunScopedTables...),
		EntityContainers:    append([]ContainerRef(nil), inventory.EntityContainers...),
		Preserved:           copyPreservedResources(preserved),
		DownstreamContracts: append([]DownstreamContract(nil), contracts...),
		ResetSeams:          append([]ResetSeam(nil), seams...),
	}, nil
}

func hasPreservedResources(p PreservedResources) bool {
	return len(p.SystemContainers) > 0 ||
		p.OperatorManagedBoundary != "" ||
		p.SchemaMigrations ||
		p.AuthTokens ||
		p.BundleContracts
}

func copyPreservedResources(p PreservedResources) PreservedResources {
	p.SystemContainers = append([]string(nil), p.SystemContainers...)
	return p
}
