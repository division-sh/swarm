package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func (t CompiledTransition) Validate() error {
	e := t.edge
	flow, err := runtimeidentity.AdmitFlowIdentity(t.flow)
	if err != nil || e.From == "" || e.To == "" || e.From != strings.TrimSpace(e.From) || e.To != strings.TrimSpace(e.To) {
		return fmt.Errorf("compiled transition requires exact flow/source/target")
	}
	if e.Node.Valid() {
		if e.Node.FlowPath() != flow.String() || e.HandlerEvent == "" || e.HandlerEvent != strings.TrimSpace(e.HandlerEvent) || e.InternalOwner != "" {
			return fmt.Errorf("compiled transition handler ownership contradicts flow")
		}
		if e.LoopID == "" && e.LoopOperation != "" {
			return fmt.Errorf("compiled transition operation requires loop identity")
		}
		if e.LoopID != "" {
			switch e.LoopOperation {
			case LoopOperationStart, LoopOperationAdmit, LoopOperationRepeat, LoopOperationClose:
			default:
				return fmt.Errorf("compiled transition requires known loop operation")
			}
		}
		if e.AdvanceCarrier == HandlerAdvanceCarrierJoinTimeout {
			if e.TimerID == "" || e.EventType != "platform.join_timeout" || !e.Timed {
				return fmt.Errorf("compiled join timeout requires exact protocol/timer identity")
			}
		} else if e.EventType != e.HandlerEvent || e.TimerID != "" || e.After != "" || e.Timed {
			return fmt.Errorf("compiled handler event/timer identity contradicts carrier")
		}
		if e.Source == "loop.escape" {
			if e.LoopID == "" || e.LoopOperation != LoopOperationRepeat || e.AdvanceCarrier != "" || e.RuleRef.Valid() {
				return fmt.Errorf("compiled escape has contradictory carrier")
			}
		} else {
			switch e.AdvanceCarrier {
			case HandlerAdvanceCarrierHandler:
				if e.RuleRef.Valid() {
					return fmt.Errorf("handler advance cannot own a rule reference")
				}
			case HandlerAdvanceCarrierRules, HandlerAdvanceCarrierOnComplete, HandlerAdvanceCarrierJoinOnComplete, HandlerAdvanceCarrierJoinTimeout:
				if !e.RuleRef.Valid() || e.RuleRef.Flow().String() != flow.String() || e.RuleRef.Family() != "handler_rule" {
					return fmt.Errorf("rule advance requires exact qualified rule")
				}
			default:
				return fmt.Errorf("compiled transition has unknown advance carrier")
			}
			wantSource := string(e.AdvanceCarrier)
			if e.LoopID != "" {
				wantSource = "loop." + string(e.LoopOperation)
			}
			if e.Source != wantSource {
				return fmt.Errorf("compiled transition source contradicts carrier")
			}
		}
		if e.DecisionID != "" || e.Verdict != "" {
			return fmt.Errorf("handler transition cannot carry gate identity")
		}
	} else {
		if e.InternalOwner != "runtime" || e.HandlerEvent != "" || e.RuleRef.Valid() || e.AdvanceCarrier != "" || e.LoopID != "" || e.LoopOperation != "" {
			return fmt.Errorf("runtime transition has contradictory handler identity")
		}
		switch e.Source {
		case "timer":
			if e.TimerID == "" || e.DecisionID != "" || e.Verdict != "" || e.EventType != "timer:"+e.TimerID || !e.Timed {
				return fmt.Errorf("timer transition requires exact timer")
			}
		case "gate":
			if e.DecisionID == "" || e.Verdict == "" || e.TimerID != "" || e.EventType != "mailbox.card_decided" || e.Timed || e.After != "" {
				return fmt.Errorf("gate transition requires exact decision/verdict")
			}
		default:
			return fmt.Errorf("unknown compiled runtime transition source")
		}
	}
	return nil
}

type compiledTransitionWire struct {
	Flow, From, To, Source, Node, InternalOwner, HandlerEvent, EventType string
	AdvanceCarrier                                                       HandlerAdvanceCarrierKind
	RuleRef                                                              string
	LoopID                                                               string
	LoopOperation                                                        LoopOperationKind
	TimerID, After                                                       string
	Timed                                                                bool
	DecisionID, Verdict                                                  string
}

func (t CompiledTransition) MarshalJSON() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	e := t.edge
	return json.Marshal(compiledTransitionWire{Flow: t.flow, From: e.From, To: e.To, Source: e.Source, Node: e.Node.Key(), InternalOwner: e.InternalOwner, HandlerEvent: e.HandlerEvent, EventType: e.EventType, AdvanceCarrier: e.AdvanceCarrier, RuleRef: e.RuleRef.Key(), LoopID: e.LoopID, LoopOperation: e.LoopOperation, TimerID: e.TimerID, After: e.After, Timed: e.Timed, DecisionID: e.DecisionID, Verdict: e.Verdict})
}

func (t *CompiledTransition) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var w compiledTransitionWire
	if err := decoder.Decode(&w); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("compiled transition has trailing JSON")
	}
	e := WorkflowStageTopologyEdge{From: w.From, To: w.To, Source: w.Source, InternalOwner: w.InternalOwner, HandlerEvent: w.HandlerEvent, EventType: w.EventType, AdvanceCarrier: w.AdvanceCarrier, LoopID: w.LoopID, LoopOperation: w.LoopOperation, TimerID: w.TimerID, After: w.After, Timed: w.Timed, DecisionID: w.DecisionID, Verdict: w.Verdict}
	var err error
	if w.Node != "" {
		e.Node, err = runtimeidentity.ParseExecutableNodeKey(w.Node)
		if err != nil {
			return err
		}
	}
	if w.RuleRef != "" {
		e.RuleRef, err = runtimeidentity.ParseDeclarationIdentityKey(w.RuleRef)
		if err != nil {
			return err
		}
	}
	value := CompiledTransition{flow: w.Flow, edge: e}
	if err := value.Validate(); err != nil {
		return err
	}
	*t = value
	return nil
}
