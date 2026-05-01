package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
)

const (
	SelectedContractRoutePersistenceOwner = "store.run_fork.selected_contract_route_persistence"
	SelectedContractRouteRecoveryOwner    = "runtime.run_fork.selected_contract_route_recovery"

	selectedContractExecutionOwner         = "runtime.run_fork.selected_contract_execution"
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
	recipientGuard    selectedContractRecoveredRecipientGuard
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

type selectedContractRecoveredRecipient struct {
	SubscriberType string `json:"subscriber_type,omitempty"`
	SubscriberID   string `json:"subscriber_id,omitempty"`
	Path           string `json:"path,omitempty"`
	RouteSource    string `json:"route_source,omitempty"`
}

type selectedContractRecoveredRecipientGuard struct {
	plansBySourceEvent map[string]selectedContractRecoveredRecipientPlanEvent
	sourceByForkEvent  map[string]string
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
		recipientGuard:    newSelectedContractRecoveredRecipientGuard(planning),
	}, nil
}

func newSelectedContractRecoveredRecipientGuard(planning selectedContractRecoveredRecipientPlanning) selectedContractRecoveredRecipientGuard {
	plans := map[string]selectedContractRecoveredRecipientPlanEvent{}
	for _, event := range planning.RecipientPlanEvents {
		sourceEventID := strings.TrimSpace(event.SourceEventID)
		if sourceEventID == "" {
			continue
		}
		plans[sourceEventID] = event
	}
	return selectedContractRecoveredRecipientGuard{
		plansBySourceEvent: plans,
		sourceByForkEvent:  map[string]string{},
	}
}

func (g *selectedContractRecoveredRecipientGuard) ExpectForkEvent(forkEventID, sourceEventID string) {
	if g == nil {
		return
	}
	forkEventID = strings.TrimSpace(forkEventID)
	sourceEventID = strings.TrimSpace(sourceEventID)
	if forkEventID == "" || sourceEventID == "" {
		return
	}
	g.sourceByForkEvent[forkEventID] = sourceEventID
}

func (g *selectedContractRecoveredRecipientGuard) AuthorizeEvent(ctx context.Context, evt events.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(evt.SourceAgent) != selectedContractExecutionOwner {
		return nil
	}
	_, _, err := g.expectedRecipientPlanEvent(evt)
	return err
}

func (g *selectedContractRecoveredRecipientGuard) Authorize(ctx context.Context, evt events.Event, actual runtimebus.PublishRecipientPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(evt.SourceAgent) != selectedContractExecutionOwner {
		return nil
	}
	sourceEventID, expected, err := g.expectedRecipientPlanEvent(evt)
	if err != nil {
		return err
	}
	if len(actual.SubscriptionRecipients) > 0 {
		return fmt.Errorf("selected-contract recovered route truth cannot use live subscriptions as fork recipient truth")
	}
	if !selectedContractRecoveredRecipientKeysEqual(selectedContractRecoveredExpectedRecipientKeys(expected.Recipients), selectedContractRecoveredActualRecipientKeys(actual.RoutedRecipients)) {
		return fmt.Errorf("selected-contract recovered routed recipients do not match %s for source event %s", selectedContractRecipientPlanningOwner, sourceEventID)
	}
	return nil
}

func (g *selectedContractRecoveredRecipientGuard) expectedRecipientPlanEvent(evt events.Event) (string, selectedContractRecoveredRecipientPlanEvent, error) {
	if g == nil {
		return "", selectedContractRecoveredRecipientPlanEvent{}, fmt.Errorf("selected-contract recovered recipient guard is required")
	}
	forkEventID := strings.TrimSpace(evt.ID)
	sourceEventID := strings.TrimSpace(g.sourceByForkEvent[forkEventID])
	if sourceEventID == "" {
		return "", selectedContractRecoveredRecipientPlanEvent{}, fmt.Errorf("selected-contract recovered publish path missing %s evidence for fork event %s", selectedContractRecipientPlanningOwner, forkEventID)
	}
	expected, ok := g.plansBySourceEvent[sourceEventID]
	if !ok {
		return "", selectedContractRecoveredRecipientPlanEvent{}, fmt.Errorf("selected-contract recovered publish path has no recipient plan for source event %s", sourceEventID)
	}
	if strings.TrimSpace(expected.EventName) != strings.TrimSpace(string(evt.Type)) {
		return "", selectedContractRecoveredRecipientPlanEvent{}, fmt.Errorf("selected-contract recovered publish event type mismatch for source event %s: got %q want %q", sourceEventID, evt.Type, expected.EventName)
	}
	return sourceEventID, expected, nil
}

func selectedContractRecoveredExpectedRecipientKeys(in []selectedContractRecoveredRecipient) []string {
	out := make([]string, 0, len(in))
	for _, recipient := range in {
		if strings.TrimSpace(recipient.SubscriberType) == "" || strings.TrimSpace(recipient.SubscriberID) == "" {
			continue
		}
		out = append(out, selectedContractRecoveredRecipientKey(recipient))
	}
	sort.Strings(out)
	return out
}

func selectedContractRecoveredActualRecipientKeys(in []runtimebus.PublishDiagnosticRecipient) []string {
	out := make([]string, 0, len(in))
	for _, recipient := range in {
		if strings.TrimSpace(recipient.Type) == "" || strings.TrimSpace(recipient.ID) == "" {
			continue
		}
		out = append(out, selectedContractRecoveredRecipientKey(selectedContractRecoveredRecipient{
			SubscriberType: recipient.Type,
			SubscriberID:   recipient.ID,
			Path:           recipient.Path,
			RouteSource:    recipient.RouteSource,
		}))
	}
	sort.Strings(out)
	return out
}

func selectedContractRecoveredRecipientKeysEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func selectedContractRecoveredRecipientKey(recipient selectedContractRecoveredRecipient) string {
	return strings.Join([]string{
		strings.TrimSpace(recipient.SubscriberType),
		strings.TrimSpace(recipient.SubscriberID),
		strings.TrimSpace(recipient.Path),
		strings.TrimSpace(recipient.RouteSource),
	}, "\x00")
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

func (am *AgentManager) SelectedContractRouteRecoveryRecipientGuard(forkRunID string) (selectedContractRecoveredRecipientGuard, bool) {
	if am == nil {
		return selectedContractRecoveredRecipientGuard{}, false
	}
	am.mu.RLock()
	defer am.mu.RUnlock()
	truth, ok := am.selectedContractRouteRecoveries[strings.TrimSpace(forkRunID)]
	if !ok {
		return selectedContractRecoveredRecipientGuard{}, false
	}
	guard := selectedContractRecoveredRecipientGuard{
		plansBySourceEvent: map[string]selectedContractRecoveredRecipientPlanEvent{},
		sourceByForkEvent:  map[string]string{},
	}
	for sourceEventID, plan := range truth.recipientGuard.plansBySourceEvent {
		guard.plansBySourceEvent[sourceEventID] = plan
	}
	return guard, true
}
