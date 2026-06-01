package runforkexecution

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store"
)

type SelectedContractRouteRecoveryReader interface {
	ListRunForkSelectedContractRouteRecoveries(ctx context.Context) ([]store.RunForkSelectedContractRouteRecovery, error)
}

func RecoverSelectedContractRouteTruth(ctx context.Context, reader SelectedContractRouteRecoveryReader) ([]store.RunForkSelectedContractRouteRecovery, error) {
	if reader == nil {
		return nil, fmt.Errorf("selected-contract route recovery requires persistence reader")
	}
	records, err := reader.ListRunForkSelectedContractRouteRecoveries(ctx)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if err := validateSelectedContractRouteRecoveryRecord(record); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func validateSelectedContractRouteRecoveryRecord(record store.RunForkSelectedContractRouteRecovery) error {
	if strings.TrimSpace(record.Owner) != store.RunForkSelectedContractRoutePersistenceOwner {
		return fmt.Errorf("selected-contract route recovery requires %s owner; got %q", store.RunForkSelectedContractRoutePersistenceOwner, record.Owner)
	}
	if strings.TrimSpace(record.RuntimeRecoveryOwner) != store.RunForkSelectedContractRouteRecoveryOwner {
		return fmt.Errorf("selected-contract route recovery requires %s runtime owner; got %q", store.RunForkSelectedContractRouteRecoveryOwner, record.RuntimeRecoveryOwner)
	}
	if strings.TrimSpace(record.ForkRunID) == "" || strings.TrimSpace(record.SourceRunID) == "" || strings.TrimSpace(record.ForkEventID) == "" {
		return fmt.Errorf("selected-contract route recovery requires fork/source/event identity")
	}
	if strings.TrimSpace(record.RouteTopologyOwner) != store.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract route recovery requires %s topology; got %q", store.RunForkSelectedContractRouteTopologyOwner, record.RouteTopologyOwner)
	}
	if strings.TrimSpace(record.RecipientPlanningOwner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract route recovery requires %s recipient planning; got %q", store.RunForkSelectedContractRecipientPlanningOwner, record.RecipientPlanningOwner)
	}
	if strings.TrimSpace(record.FrontierEvidenceFingerprint) == "" ||
		strings.TrimSpace(record.RouteTopologyFingerprint) == "" ||
		strings.TrimSpace(record.RecipientPlanningFingerprint) == "" {
		return fmt.Errorf("selected-contract route recovery requires evidence fingerprints")
	}
	if len(record.RouteTopology) == 0 || len(record.RecipientPlanning) == 0 {
		return fmt.Errorf("selected-contract route recovery requires persisted topology and recipient planning evidence")
	}
	return nil
}
