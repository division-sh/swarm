package manager

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	SelectedContractRoutePersistenceOwner = "store.run_fork.selected_contract_route_persistence"
	SelectedContractRouteRecoveryOwner    = "runtime.run_fork.selected_contract_route_recovery"
)

type SelectedContractRouteRecoveryRecord struct {
	Owner                        string
	RuntimeRecoveryOwner         string
	ForkRunID                    string
	SourceRunID                  string
	ForkEventID                  string
	RouteTopologyOwner           string
	DynamicTopologyOwner         string
	RecipientPlanningOwner       string
	FrontierEvidenceFingerprint  string
	RouteTopologyFingerprint     string
	RecipientPlanningFingerprint string
	StaticRouteEventCount        int
	DynamicTopologyProofCount    int
	RecipientPlanEventCount      int
	CreatedAt                    time.Time
}

type selectedContractRouteRecoveryLister interface {
	ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]SelectedContractRouteRecoveryRecord, error)
}

func (am *AgentManager) restoreSelectedContractRouteRecoveries(ctx context.Context) error {
	if am == nil || am.bus == nil || am.bus.Store() == nil {
		return nil
	}
	lister, ok := am.bus.Store().(selectedContractRouteRecoveryLister)
	if !ok || lister == nil {
		return nil
	}
	records, err := lister.ListSelectedContractRouteRecoveryRecords(ctx)
	if err != nil {
		return fmt.Errorf("list selected-contract route recoveries: %w", err)
	}
	recovered := make(map[string]SelectedContractRouteRecoveryRecord, len(records))
	for _, record := range records {
		if err := validateSelectedContractRouteRecoveryRecord(record); err != nil {
			return err
		}
		recovered[strings.TrimSpace(record.ForkRunID)] = record
	}
	am.mu.Lock()
	am.selectedContractRouteRecoveries = recovered
	am.mu.Unlock()
	return nil
}

func validateSelectedContractRouteRecoveryRecord(record SelectedContractRouteRecoveryRecord) error {
	if strings.TrimSpace(record.Owner) != SelectedContractRoutePersistenceOwner {
		return fmt.Errorf("selected-contract route recovery requires %s owner; got %q", SelectedContractRoutePersistenceOwner, record.Owner)
	}
	if strings.TrimSpace(record.RuntimeRecoveryOwner) != SelectedContractRouteRecoveryOwner {
		return fmt.Errorf("selected-contract route recovery requires %s runtime owner; got %q", SelectedContractRouteRecoveryOwner, record.RuntimeRecoveryOwner)
	}
	if strings.TrimSpace(record.ForkRunID) == "" ||
		strings.TrimSpace(record.SourceRunID) == "" ||
		strings.TrimSpace(record.ForkEventID) == "" {
		return fmt.Errorf("selected-contract route recovery requires fork/source/event identity")
	}
	if strings.TrimSpace(record.RouteTopologyOwner) != "runtime.run_fork.selected_contract_route_topology" {
		return fmt.Errorf("selected-contract route recovery requires route topology owner; got %q", record.RouteTopologyOwner)
	}
	if strings.TrimSpace(record.RecipientPlanningOwner) != "runtime.run_fork.selected_contract_recipient_planning" {
		return fmt.Errorf("selected-contract route recovery requires recipient planning owner; got %q", record.RecipientPlanningOwner)
	}
	if strings.TrimSpace(record.FrontierEvidenceFingerprint) == "" ||
		strings.TrimSpace(record.RouteTopologyFingerprint) == "" ||
		strings.TrimSpace(record.RecipientPlanningFingerprint) == "" {
		return fmt.Errorf("selected-contract route recovery requires evidence fingerprints")
	}
	return nil
}

func (am *AgentManager) SelectedContractRouteRecoverySnapshot() map[string]SelectedContractRouteRecoveryRecord {
	if am == nil {
		return nil
	}
	am.mu.RLock()
	defer am.mu.RUnlock()
	out := make(map[string]SelectedContractRouteRecoveryRecord, len(am.selectedContractRouteRecoveries))
	for forkRunID, record := range am.selectedContractRouteRecoveries {
		out[forkRunID] = record
	}
	return out
}
