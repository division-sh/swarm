package runforkrevision

import (
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// Bound SQL parameters and predicate depth without changing transaction scope.
const exactFactReadBatch = 128

// FactRef retains admitted coordinates. Composite keys are serialized only by
// the existing fact-key owner and are never parsed to recover SQL coordinates.
type FactRef struct {
	family      Family
	key         string
	runID       string
	coordinates factKeyCoordinates
}

func NewFactRef(family Family, key string) (FactRef, error) {
	if family == FamilyFanOutObligations {
		return FactRef{}, fmt.Errorf("fan-out revision effects require structured coordinates")
	}
	if !utf8.ValidString(key) {
		return FactRef{}, fmt.Errorf("exact revision fact key requires valid UTF-8")
	}
	return newFactRef(family, "", factKeyCoordinates{Key: key})
}

func FanOutIntentFact(key fanoutobligation.IntentKey) (FactRef, error) {
	return fanOutFactRef(key, "intent", nil)
}

func FanOutOutcomeFact(key fanoutobligation.IntentKey, ordinal int) (FactRef, error) {
	value := int64(ordinal)
	return fanOutFactRef(key, "outcome", &value)
}

func FanOutBarrierFact(key fanoutobligation.IntentKey) (FactRef, error) {
	return fanOutFactRef(key, "barrier", nil)
}

func fanOutFactRef(key fanoutobligation.IntentKey, kind string, ordinal *int64) (FactRef, error) {
	if err := key.Validate(); err != nil {
		return FactRef{}, err
	}
	if key.DeploymentFeedID != "" {
		return newFactRef(FamilyFanOutObligations, key.RunID, factKeyCoordinates{
			Kind: kind, OriginKind: "deployment", DeploymentFeedID: key.DeploymentFeedID, Ordinal: ordinal,
		})
	}
	declaration, err := key.ElementRef.DeclarationIdentity()
	if err != nil {
		return FactRef{}, err
	}
	return newFactRef(FamilyFanOutObligations, key.RunID, factKeyCoordinates{
		Kind: kind, OriginKind: "handler", TriggeringDeliveryID: key.TriggeringDeliveryID,
		FlowPath: declaration.Flow().String(), DeclarationFamily: declaration.Family(),
		SemanticPath: declaration.SemanticPath(), Ordinal: ordinal,
	})
}

func newFactRef(family Family, runID string, coordinates factKeyCoordinates) (FactRef, error) {
	key, err := admitFactKeyCoordinates(family, coordinates)
	if err != nil {
		return FactRef{}, err
	}
	return FactRef{family: family, key: key, runID: runID, coordinates: coordinates}, nil
}

func (r FactRef) validate(runID string) error {
	if r.runID != "" && r.runID != runID {
		return fmt.Errorf("exact revision fact belongs to another run")
	}
	key, err := admitFactKeyCoordinates(r.family, r.coordinates)
	if err != nil {
		return err
	}
	if key != r.key {
		return fmt.Errorf("exact revision fact coordinates differ from its key")
	}
	return nil
}

func sameFactRef(a, b FactRef) bool {
	return a.family == b.family && a.key == b.key && a.runID == b.runID && reflect.DeepEqual(a.coordinates, b.coordinates)
}
