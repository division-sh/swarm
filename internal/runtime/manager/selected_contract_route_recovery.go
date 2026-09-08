package manager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
)

const (
	SelectedContractRoutePersistenceOwner = "store.run_fork.selected_contract_route_persistence"
	SelectedContractRouteRecoveryOwner    = "runtime.run_fork.selected_contract_route_recovery"

	selectedContractRouteTopologyOwner     = "runtime.run_fork.selected_contract_route_topology"
	selectedContractRecipientPlanningOwner = "runtime.run_fork.selected_contract_recipient_planning"
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
	RouteTopology                json.RawMessage
	RecipientPlanning            json.RawMessage
	CreatedAt                    time.Time
}

type SelectedContractRouteRecoveryTruth struct {
	Record            SelectedContractRouteRecoveryRecord
	RouteTopology     selectedContractRecoveredRouteTopology
	RecipientPlanning selectedContractRecoveredRecipientPlanning
}

type selectedContractRecoveredRouteTopology struct {
	Owner                         string `json:"owner"`
	NonMutating                   bool   `json:"non_mutating"`
	RoutePersistenceSupported     bool   `json:"route_persistence_supported"`
	ExecutableRecipientsSupported bool   `json:"executable_recipients_supported"`
	FrontierEvidenceFingerprint   string `json:"frontier_evidence_fingerprint"`
	StaticRouteEvents             []struct {
		SourceEventID     string                               `json:"source_event_id,omitempty"`
		EventName         string                               `json:"event_name"`
		DerivedRecipients []selectedContractRecoveredRecipient `json:"derived_recipients,omitempty"`
		Disposition       string                               `json:"disposition"`
	} `json:"static_route_events,omitempty"`
	DynamicTopologyProofs []struct {
		FlowInstance      string                               `json:"flow_instance"`
		SourceEventIDs    []string                             `json:"source_event_ids,omitempty"`
		EventNames        []string                             `json:"event_names,omitempty"`
		DerivedRecipients []selectedContractRecoveredRecipient `json:"derived_recipients,omitempty"`
		Disposition       string                               `json:"disposition"`
	} `json:"dynamic_topology_proofs,omitempty"`
}

type selectedContractRecoveredRecipientPlanning struct {
	Owner                       string                                        `json:"owner"`
	RouteTopologyOwner          string                                        `json:"route_topology_owner"`
	NonMutating                 bool                                          `json:"non_mutating"`
	RecipientPlanningSupported  bool                                          `json:"recipient_planning_supported"`
	DeliveryWritesSupported     bool                                          `json:"delivery_writes_supported"`
	FrontierEvidenceFingerprint string                                        `json:"frontier_evidence_fingerprint"`
	RecipientPlanEvents         []selectedContractRecoveredRecipientPlanEvent `json:"recipient_plan_events,omitempty"`
}

type selectedContractRecoveredRecipientPlanEvent struct {
	SourceEventID string                               `json:"source_event_id,omitempty"`
	EventName     string                               `json:"event_name"`
	Recipients    []selectedContractRecoveredRecipient `json:"recipients,omitempty"`
	Disposition   string                               `json:"disposition"`
}

type selectedContractRecoveredRecipient = forkrecipient.Evidence

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
	recovered := make(map[string]SelectedContractRouteRecoveryTruth, len(records))
	for _, record := range records {
		truth, err := decodeSelectedContractRouteRecoveryTruth(record)
		if err != nil {
			return err
		}
		recovered[strings.TrimSpace(record.ForkRunID)] = truth
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
	if strings.TrimSpace(record.RouteTopologyOwner) != selectedContractRouteTopologyOwner {
		return fmt.Errorf("selected-contract route recovery requires route topology owner; got %q", record.RouteTopologyOwner)
	}
	if strings.TrimSpace(record.RecipientPlanningOwner) != selectedContractRecipientPlanningOwner {
		return fmt.Errorf("selected-contract route recovery requires recipient planning owner; got %q", record.RecipientPlanningOwner)
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

func decodeSelectedContractRouteRecoveryTruth(record SelectedContractRouteRecoveryRecord) (SelectedContractRouteRecoveryTruth, error) {
	if err := validateSelectedContractRouteRecoveryRecord(record); err != nil {
		return SelectedContractRouteRecoveryTruth{}, err
	}
	got, err := selectedContractRecoveredJSONFingerprint(record.RouteTopology)
	if err != nil {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("fingerprint selected-contract recovered route topology: %w", err)
	}
	if got != strings.TrimSpace(record.RouteTopologyFingerprint) {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered route topology fingerprint mismatch")
	}
	got, err = selectedContractRecoveredJSONFingerprint(record.RecipientPlanning)
	if err != nil {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("fingerprint selected-contract recovered recipient planning: %w", err)
	}
	if got != strings.TrimSpace(record.RecipientPlanningFingerprint) {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered recipient planning fingerprint mismatch")
	}
	var topology selectedContractRecoveredRouteTopology
	if err := json.Unmarshal(record.RouteTopology, &topology); err != nil {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("decode selected-contract route topology recovery: %w", err)
	}
	if strings.TrimSpace(topology.Owner) != selectedContractRouteTopologyOwner {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered route topology requires %s owner; got %q", selectedContractRouteTopologyOwner, topology.Owner)
	}
	if !topology.NonMutating || topology.RoutePersistenceSupported || topology.ExecutableRecipientsSupported {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered route topology must be non-mutating evidence without current-route persistence or executable recipients")
	}
	if strings.TrimSpace(topology.FrontierEvidenceFingerprint) != strings.TrimSpace(record.FrontierEvidenceFingerprint) {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered route topology frontier fingerprint mismatch")
	}
	var planning selectedContractRecoveredRecipientPlanning
	if err := json.Unmarshal(record.RecipientPlanning, &planning); err != nil {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("decode selected-contract recipient planning recovery: %w", err)
	}
	if strings.TrimSpace(planning.Owner) != selectedContractRecipientPlanningOwner {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered recipient planning requires %s owner; got %q", selectedContractRecipientPlanningOwner, planning.Owner)
	}
	if strings.TrimSpace(planning.RouteTopologyOwner) != selectedContractRouteTopologyOwner {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered recipient planning must consume %s; got %q", selectedContractRouteTopologyOwner, planning.RouteTopologyOwner)
	}
	if !planning.NonMutating || !planning.RecipientPlanningSupported || planning.DeliveryWritesSupported {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered recipient planning must be supported non-mutating evidence without delivery-write ownership")
	}
	if strings.TrimSpace(planning.FrontierEvidenceFingerprint) != strings.TrimSpace(record.FrontierEvidenceFingerprint) {
		return SelectedContractRouteRecoveryTruth{}, fmt.Errorf("selected-contract recovered recipient planning frontier fingerprint mismatch")
	}
	return SelectedContractRouteRecoveryTruth{
		Record:            record,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	}, nil
}

func selectedContractRecoveredJSONFingerprint(raw json.RawMessage) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", fmt.Errorf("unexpected trailing JSON")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func (am *AgentManager) SelectedContractRouteRecoverySnapshot() map[string]SelectedContractRouteRecoveryTruth {
	if am == nil {
		return nil
	}
	am.mu.RLock()
	defer am.mu.RUnlock()
	out := make(map[string]SelectedContractRouteRecoveryTruth, len(am.selectedContractRouteRecoveries))
	for forkRunID, truth := range am.selectedContractRouteRecoveries {
		out[forkRunID] = truth
	}
	return out
}
