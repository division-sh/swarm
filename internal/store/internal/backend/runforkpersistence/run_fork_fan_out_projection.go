package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func loadRunForkFanOutObligationsFromRevision(snapshot *runForkRevisionSnapshot) ([]runfork.RunForkFanOutObligation, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("run-fork fan-out projection requires revision snapshot")
	}
	type aggregate struct {
		intent   *runForkRevisionFanOutFact
		outcomes []runForkRevisionFanOutFact
	}
	byKey := make(map[string]*aggregate)
	for index := range snapshot.FanOutFacts {
		fact := snapshot.FanOutFacts[index]
		key := strings.Join([]string{fact.TriggeringDeliveryID, fact.PackageKey, fact.ElementID}, "|")
		item := byKey[key]
		if item == nil {
			item = &aggregate{}
			byKey[key] = item
		}
		switch strings.TrimSpace(fact.FactKind) {
		case "intent":
			if item.intent != nil {
				return nil, fmt.Errorf("run-fork fan-out %s has duplicate intent facts", key)
			}
			item.intent = &snapshot.FanOutFacts[index]
		case "outcome":
			item.outcomes = append(item.outcomes, fact)
		default:
			return nil, fmt.Errorf("run-fork fan-out %s has unknown fact kind %q", key, fact.FactKind)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]runfork.RunForkFanOutObligation, 0, len(keys))
	for _, key := range keys {
		aggregate := byKey[key]
		if aggregate.intent == nil {
			return nil, fmt.Errorf("run-fork fan-out %s has outcomes without intent", key)
		}
		fact := *aggregate.intent
		var capsule fanoutobligation.Capsule
		if err := json.Unmarshal(fact.Capsule, &capsule); err != nil {
			return nil, fmt.Errorf("decode run-fork fan-out %s capsule: %w", key, err)
		}
		source := fanoutobligation.SourceRef{
			Kind: fanoutobligation.SourceKind(fact.SourceKind), EventID: fact.SourceEventID, RunID: fact.SourceRunID,
			EntityID: fact.SourceEntityID, Field: fact.SourceField, MutationID: fact.SourceMutationID,
			Declaration: durabledata.DeclarationRef{PackageKey: fact.SourceResourcePackageKey, EventName: fact.SourceResourceEventName},
			VersionID:   durabledata.VersionID(fact.SourceResourceVersionID),
		}
		requestSource := source
		if requestSource.Kind == fanoutobligation.SourceEntityField {
			requestSource.MutationID = ""
		}
		element := runtimecontracts.FanOutElementRef{PackageKey: fact.PackageKey, ElementID: fact.ElementID}
		intent := fanoutobligation.Intent{
			Request: fanoutobligation.IntentRequest{
				Key:     fanoutobligation.IntentKey{RunID: snapshot.RunID, TriggeringDeliveryID: fact.TriggeringDeliveryID, ElementRef: element},
				PlanRef: runtimecontracts.FanOutPlanRef{BundleHash: fact.BundleHash, ElementRef: element, SemanticDigest: fact.SemanticDigest},
				Source:  requestSource, Cardinality: fact.Cardinality, Capsule: capsule,
			},
			Source: source, Cursor: fact.Cursor, Status: fanoutobligation.Status(fact.Status), NextChunkSize: fanoutobligation.InitialChunkSize,
			CreatedAt: fact.CreatedAt, UpdatedAt: fact.CreatedAt, BlockedReason: fact.BlockedReason,
		}
		if err := intent.Validate(); err != nil {
			return nil, fmt.Errorf("validate run-fork fan-out %s intent: %w", key, err)
		}
		sort.Slice(aggregate.outcomes, func(i, j int) bool {
			return outcomeOrdinal(aggregate.outcomes[i]) < outcomeOrdinal(aggregate.outcomes[j])
		})
		outcomes := make([]fanoutobligation.Outcome, 0, len(aggregate.outcomes))
		for index, outcomeFact := range aggregate.outcomes {
			if outcomeFact.Ordinal == nil || *outcomeFact.Ordinal != index || index >= intent.Cursor {
				return nil, fmt.Errorf("run-fork fan-out %s outcomes are not the exact contiguous cursor prefix", key)
			}
			failure := outcomeFact.Failure
			if string(failure) == "null" {
				failure = nil
			}
			outcome := fanoutobligation.Outcome{
				Ordinal: *outcomeFact.Ordinal, Kind: fanoutobligation.OutcomeKind(outcomeFact.OutcomeKind),
				EventID: outcomeFact.EventID, SourceEventID: outcomeFact.SourceOutcomeEventID, Failure: failure, CreatedAt: outcomeFact.CreatedAt,
			}
			if err := outcome.Validate(); err != nil {
				return nil, fmt.Errorf("validate run-fork fan-out %s outcome %d: %w", key, index, err)
			}
			outcomes = append(outcomes, outcome)
		}
		if len(outcomes) != intent.Cursor {
			return nil, fmt.Errorf("run-fork fan-out %s cursor %d has %d outcomes", key, intent.Cursor, len(outcomes))
		}
		out = append(out, runfork.RunForkFanOutObligation{Intent: intent, Outcomes: outcomes})
	}
	return out, nil
}

func outcomeOrdinal(fact runForkRevisionFanOutFact) int {
	if fact.Ordinal == nil {
		return -1
	}
	return *fact.Ordinal
}
