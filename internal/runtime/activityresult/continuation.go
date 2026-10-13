package activityresult

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

// PublishedContinuation separates a retained physical publication from the
// copied journal's identity. It is accounting evidence, never execution authority.
type PublishedContinuation struct {
	requestID, resultID, sourceResultID, inputHash, replyID string
	runID, resultType                                       string
	payload                                                 json.RawMessage
	mode                                                    executionmode.Mode
	completedAt                                             time.Time
	effect                                                  contracts.ActivityEffectClass
}

func NewPublishedContinuation(child activityidentity.Fact, sourceResultID, resultType string, payload json.RawMessage, mode executionmode.Mode, effect contracts.ActivityEffectClass, inputHash, replyID string, completedAt time.Time) (PublishedContinuation, error) {
	if child.RunID == "" || child.SourceEventID == "" || child.Owner.Empty() || sourceResultID == "" || resultType == "" ||
		!json.Valid(payload) || !mode.Valid() || !contracts.SupportedActivityEffectClass(effect) || inputHash == "" || completedAt.IsZero() {
		return PublishedContinuation{}, fmt.Errorf("published activity continuation requires exact source and child journal evidence")
	}
	canonical, err := canonicaljson.Canonicalize(payload)
	if err != nil {
		return PublishedContinuation{}, err
	}
	return PublishedContinuation{requestID: activityidentity.RequestEventID(child), resultID: activityidentity.ResultEventID(child, resultType),
		sourceResultID: sourceResultID, runID: child.RunID, resultType: resultType, payload: canonical,
		mode: mode, effect: effect, inputHash: inputHash, replyID: replyID, completedAt: completedAt}, nil
}

func (p PublishedContinuation) Present() bool          { return p.requestID != "" }
func (p PublishedContinuation) RequestID() string      { return p.requestID }
func (p PublishedContinuation) RunID() string          { return p.runID }
func (p PublishedContinuation) SourceResultID() string { return p.sourceResultID }

func (p PublishedContinuation) RequireInput(requestID, runID, inputHash string, mode executionmode.Mode, effect contracts.ActivityEffectClass) error {
	if !p.Present() || requestID != p.requestID || runID != p.runID || inputHash != p.inputHash || mode != p.mode || effect != p.effect {
		return fmt.Errorf("activity input differs from its fixed-cut published continuation")
	}
	return nil
}

func (p PublishedContinuation) RequireJournal(requestID, resultID, resultType string, payload json.RawMessage, mode executionmode.Mode, inputHash, replyID string, completedAt time.Time) error {
	canonical, err := canonicaljson.Canonicalize(payload)
	if err != nil {
		return err
	}
	if !p.Present() || requestID != p.requestID || resultID != p.resultID || resultType != p.resultType || mode != p.mode ||
		inputHash != p.inputHash || replyID != p.replyID || !completedAt.Equal(p.completedAt) || string(canonical) != string(p.payload) {
		return fmt.Errorf("activity journal differs from its fixed-cut published continuation")
	}
	return nil
}
