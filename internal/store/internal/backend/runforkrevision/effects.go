package runforkrevision

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// Family is the closed registry of facts that may affect supported fork
// planning at a selected event revision.
type Family string

const (
	FamilyEvents                  Family = "events"
	FamilyEntityMutations         Family = "entity_mutations"
	FamilyEntityMetadata          Family = "entity_metadata"
	FamilyEventDeliveries         Family = "event_deliveries"
	FamilyCommittedReplayScopes   Family = "committed_replay_scopes"
	FamilyEventReceipts           Family = "event_receipts"
	FamilyDeadLetters             Family = "dead_letters"
	FamilyTimers                  Family = "timers"
	FamilyAgentSessions           Family = "agent_sessions"
	FamilyAgentTurns              Family = "agent_turns"
	FamilyAgentConversationAudits Family = "agent_conversation_audits"
	FamilyReplyContexts           Family = "reply_contexts"
	FamilyFanOutObligations       Family = "fan_out_obligations"
)

var allFamilies = []Family{
	FamilyEvents,
	FamilyEntityMutations,
	FamilyEntityMetadata,
	FamilyEventDeliveries,
	FamilyCommittedReplayScopes,
	FamilyEventReceipts,
	FamilyDeadLetters,
	FamilyTimers,
	FamilyAgentSessions,
	FamilyAgentTurns,
	FamilyAgentConversationAudits,
	FamilyReplyContexts,
	FamilyFanOutObligations,
}

func AllFamilies() []Family { return append([]Family(nil), allFamilies...) }

func ValidFamily(family Family) bool {
	for _, candidate := range allFamilies {
		if family == candidate {
			return true
		}
	}
	return false
}

// Effects is the complete closed declaration made by one named selected-store
// mutation. Writers add effects after deriving the authoritative run identity;
// the outer transaction finalizes the aggregate exactly once.
type Effects struct {
	byRun map[string]map[Family]struct{}
}

func NewEffects() *Effects { return &Effects{byRun: map[string]map[Family]struct{}{}} }

func ForRun(runID string, families ...Family) (*Effects, error) {
	effects := NewEffects()
	if err := effects.Add(runID, families...); err != nil {
		return nil, err
	}
	return effects, nil
}

func (e *Effects) Add(runID string, families ...Family) error {
	if e == nil {
		return fmt.Errorf("run fork revision effects are required")
	}
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("run fork revision effect requires a UUID run_id: %w", err)
	}
	if len(families) == 0 {
		return fmt.Errorf("run fork revision effect requires at least one family")
	}
	if e.byRun == nil {
		e.byRun = map[string]map[Family]struct{}{}
	}
	if e.byRun[runID] == nil {
		e.byRun[runID] = map[Family]struct{}{}
	}
	for _, family := range families {
		family = Family(strings.TrimSpace(string(family)))
		if !ValidFamily(family) {
			return fmt.Errorf("unsupported run fork revision fact family %q", family)
		}
		e.byRun[runID][family] = struct{}{}
	}
	return nil
}

// RunIDForEvent resolves the immutable persisted run identity used by
// event-associated writers such as receipts and dead letters.
func RunIDForEvent(ctx context.Context, tx *sql.Tx, eventID string) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("event run lookup requires an existing transaction")
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return "", fmt.Errorf("event run lookup requires a UUID event_id: %w", err)
	}
	var runID sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM events WHERE event_id=$1`, eventID).Scan(&runID); err != nil {
		return "", fmt.Errorf("resolve run_id for event %s: %w", eventID, err)
	}
	return strings.TrimSpace(runID.String), nil
}

type declaredChange struct {
	runID    string
	families []Family
}

func (e *Effects) normalized() []declaredChange {
	if e == nil || len(e.byRun) == 0 {
		return nil
	}
	runIDs := make([]string, 0, len(e.byRun))
	for runID := range e.byRun {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	changes := make([]declaredChange, 0, len(runIDs))
	for _, runID := range runIDs {
		families := make([]Family, 0, len(e.byRun[runID]))
		for family := range e.byRun[runID] {
			families = append(families, family)
		}
		sort.Slice(families, func(i, j int) bool { return families[i] < families[j] })
		changes = append(changes, declaredChange{runID: runID, families: families})
	}
	return changes
}

// Result reports the revision visible after finalization. Changed is false
// when every declared canonical projection already matched the ledger.
type Result struct {
	Revision int64
	Changed  bool
}
