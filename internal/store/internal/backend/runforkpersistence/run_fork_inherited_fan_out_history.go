package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// Historical disposition consumes the same ordinal semantics as live readback,
// but its relation must exist at the selected revision, not merely in live SQL.
func admitRunForkInheritedFanOutHistory(snapshot *runForkRevisionSnapshot) error {
	for _, fact := range snapshot.Events {
		if fact.EventClass != string(events.EventAdmissionInheritedFanOut) {
			if len(fact.InheritedFanOutOrigin) != 0 && !bytes.Equal(fact.InheritedFanOutOrigin, []byte("null")) {
				return fmt.Errorf("historical event carries origin without inherited fan-out class")
			}
			continue
		}
		if fact.RunID != snapshot.RunID {
			return fmt.Errorf("historical inherited fan-out event disagrees with selected run")
		}
		admitted, err := decodeRunForkRevisionEvent(fact)
		if err != nil {
			return fmt.Errorf("historical inherited fan-out event: %w", err)
		}
		var outcome, intent *runForkRevisionFanOutFact
		for i := range snapshot.FanOutFacts {
			candidate := &snapshot.FanOutFacts[i]
			if candidate.FactKind == "outcome" && candidate.EventID == fact.EventID {
				if outcome != nil || candidate.OutcomeKind != string(fanoutobligation.OutcomeCommitted) {
					return fmt.Errorf("historical inherited event lacks one committed ordinal outcome")
				}
				outcome = candidate
			}
		}
		if outcome == nil || outcome.Ordinal == nil {
			return fmt.Errorf("historical inherited event has no ordinal owner at the fixed revision")
		}
		for i := range snapshot.FanOutFacts {
			candidate := &snapshot.FanOutFacts[i]
			if candidate.FactKind == "intent" && candidate.TriggeringDeliveryID == outcome.TriggeringDeliveryID && candidate.FlowPath == outcome.FlowPath && candidate.DeclarationFamily == outcome.DeclarationFamily && candidate.SemanticPath == outcome.SemanticPath {
				if intent != nil {
					return fmt.Errorf("historical inherited event has ambiguous intent ownership")
				}
				intent = candidate
			}
		}
		if intent == nil || *outcome.Ordinal < 0 || *outcome.Ordinal >= intent.Cursor || intent.Cursor > intent.Cardinality {
			return fmt.Errorf("historical inherited event is not in the committed intent prefix")
		}
		var capsule fanoutobligation.Capsule
		if err := json.Unmarshal(intent.Capsule, &capsule); err != nil {
			return err
		}
		element := contracts.FanOutElementRef{FlowPath: intent.FlowPath, Family: intent.DeclarationFamily, SemanticPath: intent.SemanticPath}
		key := fanoutobligation.IntentKey{RunID: snapshot.RunID, TriggeringDeliveryID: intent.TriggeringDeliveryID, ElementRef: element}
		plan := contracts.FanOutPlanRef{ElementRef: element, BundleHash: intent.BundleHash, SemanticDigest: intent.SemanticDigest}
		if err := fanoutobligation.ValidateCommittedOrdinalEvent(key, plan, capsule, *outcome.Ordinal, admitted.Event()); err != nil {
			return err
		}
	}
	return nil
}
