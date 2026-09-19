package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// ConnectPlanIdentity is the opaque identity of one compiled plan. Only the
// compiled routing owner may admit the digest; observers may only compare or
// render it.
type ConnectPlanIdentity struct{ digest [sha256.Size]byte }

func AdmitConnectPlanIdentity(digest [sha256.Size]byte) ConnectPlanIdentity {
	return ConnectPlanIdentity{digest: digest}
}

func (i ConnectPlanIdentity) Empty() bool { return i.digest == [sha256.Size]byte{} }
func (i ConnectPlanIdentity) String() string {
	if i.Empty() {
		return ""
	}
	return hex.EncodeToString(i.digest[:])
}

// ConnectReceiverIdentity is the opaque identity of one compiled receiver
// pin. It allows rejection evidence without exposing an authority-bearing pin
// spelling.
type ConnectReceiverIdentity struct{ digest [sha256.Size]byte }

func AdmitConnectReceiverIdentity(digest [sha256.Size]byte) ConnectReceiverIdentity {
	return ConnectReceiverIdentity{digest: digest}
}

func (i ConnectReceiverIdentity) Empty() bool { return i.digest == [sha256.Size]byte{} }
func (i ConnectReceiverIdentity) String() string {
	if i.Empty() {
		return ""
	}
	return hex.EncodeToString(i.digest[:])
}

type ConnectPlanResolution uint8

const (
	ConnectPlanResolved ConnectPlanResolution = iota + 1
	ConnectPlanRuntimeResolutionRequired
	ConnectPlanResolutionBlocked
	ConnectPlanNoRegistration
)

func (r ConnectPlanResolution) Code() string {
	switch r {
	case ConnectPlanResolved:
		return "resolved"
	case ConnectPlanRuntimeResolutionRequired:
		return "runtime_resolution_required"
	case ConnectPlanResolutionBlocked:
		return "resolution_blocker"
	case ConnectPlanNoRegistration:
		return "no_registration"
	default:
		return ""
	}
}

type ConnectCandidateOutcome uint8

const (
	ConnectCandidateAccepted ConnectCandidateOutcome = iota + 1
	ConnectCandidatePinMismatch
	ConnectCandidatePathMismatch
)

func (o ConnectCandidateOutcome) Code() string {
	switch o {
	case ConnectCandidateAccepted:
		return "accepted"
	case ConnectCandidatePinMismatch:
		return "pin_mismatch"
	case ConnectCandidatePathMismatch:
		return "path_mismatch"
	default:
		return ""
	}
}

// ConnectCandidateEvidence records one considered registration and exactly
// one accepted/rejected outcome.
type ConnectCandidateEvidence struct {
	receiver  ConnectReceiverIdentity
	recipient DeliveryRecipient
	path      string
	agent     agentidentity.Plan
	outcome   ConnectCandidateOutcome
}

func NewConnectCandidateEvidence(receiver ConnectReceiverIdentity, recipient DeliveryRecipient, path string, agent agentidentity.Plan, outcome ConnectCandidateOutcome) (ConnectCandidateEvidence, error) {
	path = strings.Trim(strings.TrimSpace(path), "/")
	agent = agent.Normalize()
	if receiver.Empty() || recipient.Empty() || outcome.Code() == "" {
		return ConnectCandidateEvidence{}, fmt.Errorf("connect candidate requires receiver, recipient, and outcome")
	}
	if recipient.IsAgent() {
		if err := agent.Validate(); err != nil || agent.AgentID() != recipient.ID() {
			return ConnectCandidateEvidence{}, fmt.Errorf("connect agent candidate requires its exact declaration route plan")
		}
	} else if !agent.IsZero() {
		return ConnectCandidateEvidence{}, fmt.Errorf("connect node candidate cannot carry agent plan")
	}
	return ConnectCandidateEvidence{receiver: receiver, recipient: recipient, path: path, agent: agent, outcome: outcome}, nil
}

func (e ConnectCandidateEvidence) Receiver() ConnectReceiverIdentity { return e.receiver }
func (e ConnectCandidateEvidence) Recipient() DeliveryRecipient      { return e.recipient }
func (e ConnectCandidateEvidence) Path() string                      { return e.path }
func (e ConnectCandidateEvidence) AgentPlan() agentidentity.Plan {
	return e.agent
}
func (e ConnectCandidateEvidence) Outcome() ConnectCandidateOutcome { return e.outcome }

type ConnectPlanEvaluation struct {
	planID     ConnectPlanIdentity
	resolution ConnectPlanResolution
	targets    []RouteIdentity
	candidates []ConnectCandidateEvidence
}

func NewConnectPlanEvaluation(planID ConnectPlanIdentity, resolution ConnectPlanResolution, targets []RouteIdentity, candidates []ConnectCandidateEvidence) (ConnectPlanEvaluation, error) {
	if planID.Empty() || resolution.Code() == "" {
		return ConnectPlanEvaluation{}, fmt.Errorf("connect plan evaluation requires plan identity and resolution")
	}
	targets = normalizeSettlementTargets(targets)
	var err error
	candidates, err = normalizeCandidateEvidence(candidates)
	if err != nil {
		return ConnectPlanEvaluation{}, err
	}
	switch resolution {
	case ConnectPlanNoRegistration, ConnectPlanRuntimeResolutionRequired, ConnectPlanResolutionBlocked:
		if len(candidates) != 0 {
			return ConnectPlanEvaluation{}, fmt.Errorf("connect plan-level outcome %q cannot carry candidate outcomes", resolution.Code())
		}
	case ConnectPlanResolved:
		if len(candidates) == 0 {
			return ConnectPlanEvaluation{}, fmt.Errorf("resolved connect plan requires candidate outcomes")
		}
	}
	return ConnectPlanEvaluation{planID: planID, resolution: resolution, targets: targets, candidates: candidates}, nil
}

func (e ConnectPlanEvaluation) PlanIdentity() ConnectPlanIdentity { return e.planID }
func (e ConnectPlanEvaluation) Resolution() ConnectPlanResolution { return e.resolution }
func (e ConnectPlanEvaluation) Targets() []RouteIdentity {
	return append([]RouteIdentity(nil), e.targets...)
}
func (e ConnectPlanEvaluation) Candidates() []ConnectCandidateEvidence {
	return append([]ConnectCandidateEvidence(nil), e.candidates...)
}

type ConnectEvaluationLedger struct {
	present bool
	plans   []ConnectPlanEvaluation
}

// NewConnectEvaluationLedger records that canonical compiled evaluation ran.
// An empty plan list is meaningful and remains distinct from an absent ledger.
func NewConnectEvaluationLedger(plans []ConnectPlanEvaluation) (ConnectEvaluationLedger, error) {
	plans = append([]ConnectPlanEvaluation(nil), plans...)
	sort.Slice(plans, func(i, j int) bool { return plans[i].planID.String() < plans[j].planID.String() })
	compacted := plans[:0]
	for _, plan := range plans {
		if plan.planID.Empty() || plan.resolution.Code() == "" {
			return ConnectEvaluationLedger{}, fmt.Errorf("connect evaluation ledger contains an invalid plan")
		}
		if len(compacted) > 0 && compacted[len(compacted)-1].planID == plan.planID {
			if !equalPlanEvaluation(compacted[len(compacted)-1], plan) {
				return ConnectEvaluationLedger{}, fmt.Errorf("connect evaluation ledger contains conflicting plan identity %s", plan.planID.String())
			}
			continue
		}
		compacted = append(compacted, plan)
	}
	return ConnectEvaluationLedger{present: true, plans: compacted}, nil
}

func (l ConnectEvaluationLedger) Present() bool { return l.present }
func (l ConnectEvaluationLedger) Plans() []ConnectPlanEvaluation {
	return append([]ConnectPlanEvaluation(nil), l.plans...)
}

type EventWriteClass uint8

const (
	EventWriteNormalPublication EventWriteClass = iota + 1
	EventWriteSelectedForkPublication
	EventWriteDirectiveDirect
	EventWriteRuntimeLogDirect
	EventWriteInboundEvidenceDirect
	EventWriteHistoricalRunForkReplay
)

func (c EventWriteClass) Code() string {
	switch c {
	case EventWriteNormalPublication:
		return "normal_publication"
	case EventWriteSelectedForkPublication:
		return "selected_fork_publication"
	case EventWriteDirectiveDirect:
		return "directive_direct"
	case EventWriteRuntimeLogDirect:
		return "runtime_log_direct"
	case EventWriteInboundEvidenceDirect:
		return "inbound_evidence_direct"
	case EventWriteHistoricalRunForkReplay:
		return "historical_run_fork_replay"
	default:
		return ""
	}
}

type NoDeliveryReason uint8

const (
	NoDeliveryMatchedNoRecipient NoDeliveryReason = iota + 1
	NoDeliveryResolutionBlocked
	NoDeliveryDeclaredConsumerNoPlan
	NoDeliveryNoSubscriberByDesign
)

func (r NoDeliveryReason) Code() string {
	switch r {
	case NoDeliveryMatchedNoRecipient:
		return "matched_no_recipient"
	case NoDeliveryResolutionBlocked:
		return "resolution_blocked"
	case NoDeliveryDeclaredConsumerNoPlan:
		return "declared_consumer_no_plan"
	case NoDeliveryNoSubscriberByDesign:
		return "no_subscriber_by_design"
	default:
		return ""
	}
}

type routeSettlementArm uint8

const (
	routeSettlementDelivery routeSettlementArm = iota + 1
	routeSettlementNoDelivery
)

// RouteSettlement is the immutable event-owned route/no-delivery union.
type RouteSettlement struct {
	writeClass EventWriteClass
	arm        routeSettlementArm
	reason     NoDeliveryReason
	ledger     ConnectEvaluationLedger
}

func NewDeliverySettlement(class EventWriteClass, ledger ConnectEvaluationLedger) (RouteSettlement, error) {
	settlement := RouteSettlement{writeClass: class, arm: routeSettlementDelivery, ledger: ledger}
	if err := settlement.Validate([]DeliveryRoute{{}}); err != nil {
		return RouteSettlement{}, err
	}
	return settlement, nil
}

func NewNoDeliverySettlement(class EventWriteClass, reason NoDeliveryReason, ledger ConnectEvaluationLedger) (RouteSettlement, error) {
	settlement := RouteSettlement{writeClass: class, arm: routeSettlementNoDelivery, reason: reason, ledger: ledger}
	if err := settlement.Validate(nil); err != nil {
		return RouteSettlement{}, err
	}
	return settlement, nil
}

func (s RouteSettlement) WriteClass() EventWriteClass     { return s.writeClass }
func (s RouteSettlement) Delivered() bool                 { return s.arm == routeSettlementDelivery }
func (s RouteSettlement) NoDelivery() bool                { return s.arm == routeSettlementNoDelivery }
func (s RouteSettlement) Reason() NoDeliveryReason        { return s.reason }
func (s RouteSettlement) Ledger() ConnectEvaluationLedger { return s.ledger }

func (s RouteSettlement) Validate(routes []DeliveryRoute) error {
	if s.writeClass.Code() == "" || (s.arm != routeSettlementDelivery && s.arm != routeSettlementNoDelivery) {
		return fmt.Errorf("route settlement requires a known write class and exactly one arm")
	}
	if s.arm == routeSettlementDelivery && len(routes) == 0 {
		return fmt.Errorf("delivery settlement requires at least one durable route")
	}
	if s.arm == routeSettlementNoDelivery && len(routes) != 0 {
		return fmt.Errorf("no-delivery settlement cannot carry durable routes")
	}
	switch s.writeClass {
	case EventWriteNormalPublication, EventWriteSelectedForkPublication:
		if !s.ledger.present {
			return fmt.Errorf("compiled publication settlement requires its evaluation ledger")
		}
		if s.arm == routeSettlementNoDelivery && s.reason.Code() == "" {
			return fmt.Errorf("compiled no-delivery settlement requires a reason")
		}
		if s.arm == routeSettlementDelivery && s.reason != 0 {
			return fmt.Errorf("delivery settlement cannot carry a no-delivery reason")
		}
	case EventWriteDirectiveDirect, EventWriteRuntimeLogDirect, EventWriteInboundEvidenceDirect:
		if s.arm != routeSettlementNoDelivery || s.reason != NoDeliveryNoSubscriberByDesign || s.ledger.present {
			return fmt.Errorf("direct event settlement requires deliberate no-delivery without compiled evidence")
		}
	case EventWriteHistoricalRunForkReplay:
		if s.arm != routeSettlementDelivery || s.reason != 0 || s.ledger.present {
			return fmt.Errorf("historical replay settlement requires delivery without current-plan evidence")
		}
	}
	return nil
}

type settlementCandidateWire struct {
	Receiver      string             `json:"receiver_sha256"`
	RecipientKind string             `json:"recipient_kind"`
	RecipientID   string             `json:"recipient_id"`
	Path          string             `json:"path,omitempty"`
	AgentPlan     agentidentity.Plan `json:"agent_plan,omitempty"`
	Outcome       string             `json:"outcome"`
}

type settlementPlanWire struct {
	PlanID     string                    `json:"plan_sha256"`
	Resolution string                    `json:"resolution"`
	Targets    []RouteIdentity           `json:"targets"`
	Candidates []settlementCandidateWire `json:"candidates"`
}

type routeSettlementWire struct {
	WriteClass string                `json:"write_class"`
	Arm        string                `json:"arm"`
	Reason     string                `json:"reason,omitempty"`
	Evaluation *settlementLedgerWire `json:"evaluation,omitempty"`
}

type settlementLedgerWire struct {
	Plans []settlementPlanWire `json:"plans"`
}

func (w *settlementLedgerWire) UnmarshalJSON(raw []byte) error {
	var decoded settlementLedgerWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if err := decoded.decodeObject(decoder, token); err != nil {
		return err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	*w = decoded
	return nil
}

// The root has already read the opening token to distinguish a nil evaluation
// pointer from an object. Standalone decoding passes its opening token here too.
func (w *settlementLedgerWire) decodeObject(decoder *json.Decoder, token json.Token) error {
	var decoded settlementLedgerWire
	if token != json.Delim('{') {
		return fmt.Errorf("route settlement evaluation must be an object")
	}
	// Exact-key presence is independent of case-folded struct-field matching:
	// a later alias may replace the decoded value, but not the exact-key fact.
	plansPresent := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || !strings.EqualFold(key, "plans") {
			return fmt.Errorf("json: unknown field %q", key)
		}
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		switch token {
		case nil:
			decoded.Plans = nil
		case json.Delim('['):
			decoded.Plans = decoded.Plans[:0]
			for decoder.More() {
				// Each custom plan decode starts fresh, even in duplicate arrays.
				var plan settlementPlanWire
				if err := plan.decodeObject(decoder); err != nil {
					return err
				}
				decoded.Plans = append(decoded.Plans, plan)
			}
			if _, err := decoder.Token(); err != nil {
				return err
			}
			if len(decoded.Plans) == 0 {
				decoded.Plans = []settlementPlanWire{}
			}
		default:
			return fmt.Errorf("route settlement evaluation plans must be an array")
		}
		if key == "plans" {
			plansPresent = decoded.Plans != nil
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	if !plansPresent {
		return fmt.Errorf("route settlement evaluation plans are required")
	}
	*w = decoded
	return nil
}

func (w *settlementPlanWire) UnmarshalJSON(raw []byte) error {
	var decoded settlementPlanWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoded.decodeObject(decoder); err != nil {
		return err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	*w = decoded
	return nil
}

// decodeObject also serves the ledger's array reader, avoiding a whole-array
// scan followed by a custom-unmarshaler rescan of each plan. Its caller owns EOF.
func (w *settlementPlanWire) decodeObject(decoder *json.Decoder) error {
	var decoded settlementPlanWire
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("route settlement plan must be an object")
	}
	targetsPresent, candidatesPresent := false, false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("route settlement plan field must be a string")
		}
		switch {
		case strings.EqualFold(key, "plan_sha256"):
			err = decoder.Decode(&decoded.PlanID)
		case strings.EqualFold(key, "resolution"):
			err = decoder.Decode(&decoded.Resolution)
		case strings.EqualFold(key, "targets"):
			err = decoder.Decode(&decoded.Targets)
			if key == "targets" {
				targetsPresent = decoded.Targets != nil
			}
		case strings.EqualFold(key, "candidates"):
			err = decoder.Decode(&decoded.Candidates)
			if key == "candidates" {
				candidatesPresent = decoded.Candidates != nil
			}
		default:
			return fmt.Errorf("json: unknown field %q", key)
		}
		if err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	if !targetsPresent {
		return fmt.Errorf("route settlement plan targets are required")
	}
	if !candidatesPresent {
		return fmt.Errorf("route settlement plan candidates are required")
	}
	*w = decoded
	return nil
}

func (w *routeSettlementWire) decodeObject(decoder *json.Decoder) error {
	var decoded routeSettlementWire
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		*w = decoded
		return nil
	}
	if token != json.Delim('{') {
		return fmt.Errorf("route settlement must be an object")
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("route settlement field must be a string")
		}
		switch {
		case strings.EqualFold(key, "write_class"):
			err = decoder.Decode(&decoded.WriteClass)
		case strings.EqualFold(key, "arm"):
			err = decoder.Decode(&decoded.Arm)
		case strings.EqualFold(key, "reason"):
			err = decoder.Decode(&decoded.Reason)
		case strings.EqualFold(key, "evaluation"):
			token, err = decoder.Token()
			if err == nil {
				if token == nil {
					decoded.Evaluation = nil
				} else {
					if decoded.Evaluation == nil {
						decoded.Evaluation = &settlementLedgerWire{}
					}
					err = decoded.Evaluation.decodeObject(decoder, token)
				}
			}
		default:
			return fmt.Errorf("json: unknown field %q", key)
		}
		if err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	*w = decoded
	return nil
}

func (s RouteSettlement) MarshalJSON() ([]byte, error) {
	if err := s.validateShape(); err != nil {
		return nil, err
	}
	wire := routeSettlementWire{WriteClass: s.writeClass.Code()}
	if s.Delivered() {
		wire.Arm = "delivery"
	} else {
		wire.Arm = "no_delivery"
		wire.Reason = s.reason.Code()
	}
	if s.ledger.present {
		plans := make([]settlementPlanWire, 0, len(s.ledger.plans))
		for _, plan := range s.ledger.plans {
			candidates := make([]settlementCandidateWire, 0, len(plan.candidates))
			for _, candidate := range plan.candidates {
				candidates = append(candidates, settlementCandidateWire{
					Receiver: candidate.receiver.String(), RecipientKind: candidate.recipient.Code(), RecipientID: candidate.recipient.ID(),
					Path: candidate.path, AgentPlan: candidate.agent, Outcome: candidate.outcome.Code(),
				})
			}
			plans = append(plans, settlementPlanWire{
				PlanID: plan.planID.String(), Resolution: plan.resolution.Code(),
				Targets: append([]RouteIdentity{}, plan.targets...), Candidates: candidates,
			})
		}
		wire.Evaluation = &settlementLedgerWire{Plans: plans}
	}
	return json.Marshal(wire)
}

func (s *RouteSettlement) UnmarshalJSON(raw []byte) error {
	if s == nil {
		return fmt.Errorf("route settlement destination is nil")
	}
	var wire routeSettlementWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := wire.decodeObject(decoder); err != nil {
		return fmt.Errorf("decode route settlement: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
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
	*s = restored
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func (s RouteSettlement) validateShape() error {
	routes := []DeliveryRoute(nil)
	if s.Delivered() {
		routes = []DeliveryRoute{{}}
	}
	return s.Validate(routes)
}

func normalizeSettlementTargets(targets []RouteIdentity) []RouteIdentity {
	seen := map[RouteIdentity]struct{}{}
	out := make([]RouteIdentity, 0, len(targets))
	for _, target := range targets {
		target = target.Normalized()
		if _, exists := seen[target]; exists {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FlowID != out[j].FlowID {
			return out[i].FlowID < out[j].FlowID
		}
		if out[i].FlowInstance != out[j].FlowInstance {
			return out[i].FlowInstance < out[j].FlowInstance
		}
		return out[i].EntityID < out[j].EntityID
	})
	return out
}

func normalizeCandidateEvidence(in []ConnectCandidateEvidence) ([]ConnectCandidateEvidence, error) {
	if len(in) == 0 {
		return nil, nil
	}
	type keyedCandidate struct {
		value ConnectCandidateEvidence
		key   string
	}
	ordered := make([]keyedCandidate, len(in))
	for i, candidate := range in {
		ordered[i] = keyedCandidate{value: candidate, key: candidate.receiver.String() + "\x00" + candidate.recipient.Code() + "\x00" + candidate.recipient.ID() + "\x00" + candidate.path + "\x00" + candidate.agent.Description() + "\x00" + candidate.outcome.Code()}
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].key < ordered[j].key
	})
	compacted := make([]ConnectCandidateEvidence, 0, len(in))
	for _, entry := range ordered {
		candidate := entry.value
		if len(compacted) > 0 && sameCandidateIdentity(compacted[len(compacted)-1], candidate) {
			if compacted[len(compacted)-1].outcome != candidate.outcome {
				return nil, fmt.Errorf("connect candidate has conflicting outcomes for receiver %s and recipient %s", candidate.receiver.String(), candidate.recipient.ID())
			}
			continue
		}
		compacted = append(compacted, candidate)
	}
	return compacted, nil
}

func sameCandidateIdentity(left, right ConnectCandidateEvidence) bool {
	return left.receiver == right.receiver && left.recipient == right.recipient &&
		left.path == right.path && left.agent == right.agent
}

func equalCandidate(left, right ConnectCandidateEvidence) bool {
	return left.receiver == right.receiver && left.recipient == right.recipient &&
		left.path == right.path && left.agent == right.agent && left.outcome == right.outcome
}

func equalPlanEvaluation(left, right ConnectPlanEvaluation) bool {
	if left.planID != right.planID || left.resolution != right.resolution || len(left.targets) != len(right.targets) || len(left.candidates) != len(right.candidates) {
		return false
	}
	for i := range left.targets {
		if left.targets[i] != right.targets[i] {
			return false
		}
	}
	for i := range left.candidates {
		if !equalCandidate(left.candidates[i], right.candidates[i]) {
			return false
		}
	}
	return true
}

func decodeDigest(raw string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(raw) != sha256.Size*2 || raw != strings.ToLower(raw) {
		return digest, fmt.Errorf("sha256 digest is invalid")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != sha256.Size {
		return digest, fmt.Errorf("sha256 digest is invalid")
	}
	copy(digest[:], decoded)
	if digest == [sha256.Size]byte{} {
		return digest, fmt.Errorf("sha256 digest is empty")
	}
	return digest, nil
}

func eventWriteClassFromCode(raw string) (EventWriteClass, bool) {
	for _, class := range []EventWriteClass{EventWriteNormalPublication, EventWriteSelectedForkPublication, EventWriteDirectiveDirect, EventWriteRuntimeLogDirect, EventWriteInboundEvidenceDirect, EventWriteHistoricalRunForkReplay} {
		if raw == class.Code() {
			return class, true
		}
	}
	return 0, false
}

func connectPlanResolutionFromCode(raw string) (ConnectPlanResolution, bool) {
	for _, resolution := range []ConnectPlanResolution{ConnectPlanResolved, ConnectPlanRuntimeResolutionRequired, ConnectPlanResolutionBlocked, ConnectPlanNoRegistration} {
		if raw == resolution.Code() {
			return resolution, true
		}
	}
	return 0, false
}

func connectCandidateOutcomeFromCode(raw string) (ConnectCandidateOutcome, bool) {
	for _, outcome := range []ConnectCandidateOutcome{ConnectCandidateAccepted, ConnectCandidatePinMismatch, ConnectCandidatePathMismatch} {
		if raw == outcome.Code() {
			return outcome, true
		}
	}
	return 0, false
}

func noDeliveryReasonFromCode(raw string) (NoDeliveryReason, bool) {
	for _, reason := range []NoDeliveryReason{NoDeliveryMatchedNoRecipient, NoDeliveryResolutionBlocked, NoDeliveryDeclaredConsumerNoPlan, NoDeliveryNoSubscriberByDesign} {
		if raw == reason.Code() {
			return reason, true
		}
	}
	return 0, false
}
