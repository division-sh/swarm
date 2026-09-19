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
	byRun map[string]map[Family]*familySelection
}

type familySelection struct {
	whole bool
	facts map[string]FactRef
}

func NewEffects() *Effects { return &Effects{byRun: map[string]map[Family]*familySelection{}} }

// AttemptReset is captured by the outer transaction owner before entering a
// retryable transaction. Invoke it at each callback entry: contributions from
// rolled-back attempts must not survive, while predeclared effects must.
func (e *Effects) AttemptReset() func() {
	baseline := cloneSelections(e)
	return func() {
		if e != nil {
			e.byRun = cloneSelections(&Effects{byRun: baseline})
		}
	}
}

func cloneSelections(e *Effects) map[string]map[Family]*familySelection {
	result := make(map[string]map[Family]*familySelection)
	if e == nil {
		return result
	}
	for runID, families := range e.byRun {
		result[runID] = make(map[Family]*familySelection, len(families))
		for family, selection := range families {
			copy := &familySelection{whole: selection.whole, facts: make(map[string]FactRef, len(selection.facts))}
			for key, ref := range selection.facts {
				copy.facts[key] = ref
			}
			result[runID][family] = copy
		}
	}
	return result
}

func ForRun(runID string, families ...Family) (*Effects, error) {
	effects := NewEffects()
	if err := effects.Add(runID, families...); err != nil {
		return nil, err
	}
	return effects, nil
}

// Add explicitly selects whole families for broad mutations. It dominates
// exact contributions to those families; it is never an automatic fallback.
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
	for _, family := range families {
		family = Family(strings.TrimSpace(string(family)))
		if !ValidFamily(family) {
			return fmt.Errorf("unsupported run fork revision fact family %q", family)
		}
	}
	for _, family := range families {
		family = Family(strings.TrimSpace(string(family)))
		selection := e.selection(runID, family)
		selection.whole, selection.facts = true, nil
	}
	return nil
}

// AddFacts declares the complete affected set at the canonical writer. Repeated
// contributions union by exact identity; a missing current fact must have prior
// owning-run history before the finalizer can interpret it as a deletion.
func (e *Effects) AddFacts(runID string, refs ...FactRef) error {
	if e == nil {
		return fmt.Errorf("run fork revision effects are required")
	}
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("run fork revision effect requires a UUID run_id: %w", err)
	}
	if len(refs) == 0 {
		return fmt.Errorf("exact revision effect requires at least one fact")
	}
	type coordinate struct {
		family Family
		key    string
	}
	pending := make(map[coordinate]FactRef, len(refs))
	for _, ref := range refs {
		if err := ref.validate(runID); err != nil {
			return err
		}
		key := coordinate{ref.family, ref.key}
		if prior, exists := pending[key]; exists && !sameFactRef(prior, ref) {
			return fmt.Errorf("exact revision effect has conflicting coordinates for %s", ref.key)
		}
		if selection := e.byRun[runID][ref.family]; selection != nil && !selection.whole {
			if prior, exists := selection.facts[ref.key]; exists && !sameFactRef(prior, ref) {
				return fmt.Errorf("exact revision effect has conflicting coordinates for %s", ref.key)
			}
		}
		pending[key] = ref
	}
	for _, ref := range refs {
		selection := e.selection(runID, ref.family)
		if selection.whole {
			continue
		}
		selection.facts[ref.key] = ref
	}
	return nil
}

// AddFact is the scalar-coordinate form of AddFacts, not a separate capture path.
func (e *Effects) AddFact(runID string, family Family, key string) error {
	ref, err := NewFactRef(family, key)
	if err != nil {
		return err
	}
	return e.AddFacts(runID, ref)
}

func (e *Effects) selection(runID string, family Family) *familySelection {
	if e.byRun == nil {
		e.byRun = map[string]map[Family]*familySelection{}
	}
	if e.byRun[runID] == nil {
		e.byRun[runID] = map[Family]*familySelection{}
	}
	if e.byRun[runID][family] == nil {
		e.byRun[runID][family] = &familySelection{facts: map[string]FactRef{}}
	}
	return e.byRun[runID][family]
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
	exact    map[Family][]FactRef
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
		exact := make(map[Family][]FactRef)
		for family, selection := range e.byRun[runID] {
			families = append(families, family)
			if !selection.whole {
				refs := make([]FactRef, 0, len(selection.facts))
				for _, ref := range selection.facts {
					refs = append(refs, ref)
				}
				sort.Slice(refs, func(i, j int) bool { return refs[i].key < refs[j].key })
				exact[family] = refs
			}
		}
		sort.Slice(families, func(i, j int) bool { return families[i] < families[j] })
		changes = append(changes, declaredChange{runID: runID, families: families, exact: exact})
	}
	return changes
}

// Result reports the revision visible after finalization. Changed is false
// when every declared canonical projection already matched the ledger.
type Result struct {
	Revision int64
	Changed  bool
}
