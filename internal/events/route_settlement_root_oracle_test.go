package events

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type routeSettlementRootBefore RouteSettlement

type routeSettlementRootWireBefore struct {
	WriteClass string                      `json:"write_class"`
	Arm        string                      `json:"arm"`
	Reason     string                      `json:"reason,omitempty"`
	Evaluation *settlementLedgerWireBefore `json:"evaluation,omitempty"`
}

func (s *routeSettlementRootBefore) UnmarshalJSON(raw []byte) error {
	return s.unmarshalJSON(raw, false)
}

// The default oracle keeps the original whole-root typed decode, original
// two-pass ledger/plan decoders, and the original semantic restoration below.
// The benchmark-only beforeRoot arm uses the qualified ledger array decoder to
// isolate this root change from the earlier private wire improvements.
func (s *routeSettlementRootBefore) unmarshalJSON(raw []byte, beforeRoot bool) error {
	if s == nil {
		return fmt.Errorf("route settlement destination is nil")
	}
	var wire routeSettlementWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if beforeRoot {
		if err := decoder.Decode(&wire); err != nil {
			return fmt.Errorf("decode route settlement: %w", err)
		}
	} else {
		var original routeSettlementRootWireBefore
		if err := decoder.Decode(&original); err != nil {
			return fmt.Errorf("decode route settlement: %w", err)
		}
		wire.WriteClass, wire.Arm, wire.Reason = original.WriteClass, original.Arm, original.Reason
		if original.Evaluation != nil {
			wire.Evaluation = &settlementLedgerWire{}
			if original.Evaluation.Plans != nil {
				wire.Evaluation.Plans = make([]settlementPlanWire, len(original.Evaluation.Plans))
				for i, plan := range original.Evaluation.Plans {
					wire.Evaluation.Plans[i] = settlementPlanWire(plan)
				}
			}
		}
	}
	if err := settlementWireBeforeEOF(decoder); err != nil {
		return fmt.Errorf("decode route settlement: %w", err)
	}
	class, ok := eventWriteClassFromCode(wire.WriteClass)
	if !ok {
		return fmt.Errorf("route settlement write class %q is invalid", wire.WriteClass)
	}
	var ledger ConnectEvaluationLedger
	if wire.Evaluation != nil {
		plans := make([]ConnectPlanEvaluation, 0, len(wire.Evaluation.Plans))
		for _, encoded := range wire.Evaluation.Plans {
			planDigest, err := decodeDigest(encoded.PlanID)
			if err != nil {
				return fmt.Errorf("route settlement plan identity: %w", err)
			}
			resolution, ok := connectPlanResolutionFromCode(encoded.Resolution)
			if !ok {
				return fmt.Errorf("route settlement plan resolution %q is invalid", encoded.Resolution)
			}
			candidates := make([]ConnectCandidateEvidence, 0, len(encoded.Candidates))
			for _, candidate := range encoded.Candidates {
				receiverDigest, err := decodeDigest(candidate.Receiver)
				if err != nil {
					return fmt.Errorf("route settlement candidate receiver: %w", err)
				}
				kind, ok := deliveryRecipientKindFromCode(candidate.RecipientKind)
				if !ok {
					return fmt.Errorf("route settlement candidate recipient kind is invalid")
				}
				recipient, err := newDeliveryRecipient(kind, candidate.RecipientID)
				if err != nil {
					return err
				}
				outcome, ok := connectCandidateOutcomeFromCode(candidate.Outcome)
				if !ok {
					return fmt.Errorf("route settlement candidate outcome %q is invalid", candidate.Outcome)
				}
				evidence, err := NewConnectCandidateEvidence(AdmitConnectReceiverIdentity(receiverDigest), recipient, candidate.Path, candidate.AgentPlan, outcome)
				if err != nil {
					return err
				}
				candidates = append(candidates, evidence)
			}
			plan, err := NewConnectPlanEvaluation(AdmitConnectPlanIdentity(planDigest), resolution, encoded.Targets, candidates)
			if err != nil {
				return err
			}
			plans = append(plans, plan)
		}
		var err error
		ledger, err = NewConnectEvaluationLedger(plans)
		if err != nil {
			return err
		}
	}
	var restored RouteSettlement
	switch wire.Arm {
	case "delivery":
		if wire.Reason != "" {
			return fmt.Errorf("delivery settlement cannot carry reason")
		}
		restored = RouteSettlement{writeClass: class, arm: routeSettlementDelivery, ledger: ledger}
	case "no_delivery":
		reason, ok := noDeliveryReasonFromCode(wire.Reason)
		if !ok {
			return fmt.Errorf("route settlement reason %q is invalid", wire.Reason)
		}
		restored = RouteSettlement{writeClass: class, arm: routeSettlementNoDelivery, reason: reason, ledger: ledger}
	default:
		return fmt.Errorf("route settlement arm %q is invalid", wire.Arm)
	}
	if err := restored.validateShape(); err != nil {
		return err
	}
	*s = routeSettlementRootBefore(restored)
	return nil
}
