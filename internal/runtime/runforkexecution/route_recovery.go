package runforkexecution

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type SelectedContractRouteRecoveryReader interface {
	ListRunForkSelectedContractRouteRecoveries(ctx context.Context) ([]runfork.RunForkSelectedContractRouteRecovery, error)
}

func RecoverSelectedContractRouteTruth(ctx context.Context, reader SelectedContractRouteRecoveryReader) ([]runfork.RunForkSelectedContractRouteRecovery, error) {
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

func validateSelectedContractRouteRecoveryRecord(record runfork.RunForkSelectedContractRouteRecovery) error {
	if strings.TrimSpace(record.Owner) != runfork.RunForkSelectedContractRoutePersistenceOwner {
		return fmt.Errorf("selected-contract route recovery requires %s owner; got %q", runfork.RunForkSelectedContractRoutePersistenceOwner, record.Owner)
	}
	if strings.TrimSpace(record.RuntimeRecoveryOwner) != runfork.RunForkSelectedContractRouteRecoveryOwner {
		return fmt.Errorf("selected-contract route recovery requires %s runtime owner; got %q", runfork.RunForkSelectedContractRouteRecoveryOwner, record.RuntimeRecoveryOwner)
	}
	if strings.TrimSpace(record.ForkRunID) == "" || strings.TrimSpace(record.SourceRunID) == "" || strings.TrimSpace(record.ForkEventID) == "" {
		return fmt.Errorf("selected-contract route recovery requires fork/source/event identity")
	}
	if strings.TrimSpace(record.RouteTopologyOwner) != runfork.RunForkSelectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract route recovery requires %s topology; got %q", runfork.RunForkSelectedContractRouteTopologyOwner, record.RouteTopologyOwner)
	}
	if strings.TrimSpace(record.RecipientPlanningOwner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract route recovery requires %s recipient planning; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, record.RecipientPlanningOwner)
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
