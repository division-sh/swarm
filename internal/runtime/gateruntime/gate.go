package gateruntime

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const BucketKey = "stage_gates"

type Status string

const (
	StatusOpen              Status = "open"
	StatusDecisionCommitted Status = "decision_committed"
	StatusRouted            Status = "routed"
	StatusSuperseded        Status = "superseded"
)

type Activation struct {
	FlowID           string    `json:"flow_id,omitempty"`
	Stage            string    `json:"stage"`
	DecisionID       string    `json:"decision_id"`
	ActivationID     string    `json:"activation_id"`
	CardID           string    `json:"card_id"`
	BundleHash       string    `json:"bundle_hash"`
	RoutesJSON       string    `json:"routes_json"`
	Status           Status    `json:"status"`
	StartedByEvent   string    `json:"started_by_event,omitempty"`
	DecisionEventID  string    `json:"decision_event_id,omitempty"`
	SupersededReason string    `json:"superseded_reason,omitempty"`
	OpenedAt         time.Time `json:"opened_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func New(runID, flowInstance, entityID, flowID, stage, decisionID, bundleHash, routesJSON, sourceEvent string, enteredAt time.Time) (Activation, error) {
	if enteredAt.IsZero() {
		enteredAt = time.Now().UTC()
	}
	identity := strings.Join([]string{
		strings.TrimSpace(runID), strings.Trim(strings.TrimSpace(flowInstance), "/"), strings.TrimSpace(entityID),
		strings.TrimSpace(flowID), strings.TrimSpace(stage), strings.TrimSpace(decisionID), strings.TrimSpace(sourceEvent),
		enteredAt.UTC().Format(time.RFC3339Nano),
	}, "\x00")
	activationID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.gate.activation.v1\x00"+identity)).String()
	cardIdentity := strings.Join([]string{
		strings.TrimSpace(runID), strings.Trim(strings.TrimSpace(flowInstance), "/"), strings.TrimSpace(entityID), activationID, strings.TrimSpace(decisionID),
	}, "\x00")
	activation := Activation{
		FlowID: strings.TrimSpace(flowID), Stage: strings.TrimSpace(stage), DecisionID: strings.TrimSpace(decisionID),
		ActivationID: activationID, CardID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.decision.card.v1\x00"+cardIdentity)).String(),
		BundleHash: strings.TrimSpace(bundleHash), RoutesJSON: strings.TrimSpace(routesJSON), Status: StatusOpen, StartedByEvent: strings.TrimSpace(sourceEvent),
		OpenedAt: enteredAt.UTC(), UpdatedAt: enteredAt.UTC(),
	}
	if err := activation.Validate(); err != nil {
		return Activation{}, err
	}
	return activation, nil
}

func (a Activation) Validate() error {
	if strings.TrimSpace(a.Stage) == "" || strings.TrimSpace(a.DecisionID) == "" || strings.TrimSpace(a.ActivationID) == "" || strings.TrimSpace(a.CardID) == "" || strings.TrimSpace(a.BundleHash) == "" {
		return fmt.Errorf("gate activation identity is incomplete")
	}
	if err := ValidateRoutes(a.RoutesJSON); err != nil {
		return err
	}
	if _, err := uuid.Parse(a.ActivationID); err != nil {
		return fmt.Errorf("gate activation id is invalid: %w", err)
	}
	if _, err := uuid.Parse(a.CardID); err != nil {
		return fmt.Errorf("gate card id is invalid: %w", err)
	}
	switch a.Status {
	case StatusOpen, StatusDecisionCommitted, StatusRouted, StatusSuperseded:
	default:
		return fmt.Errorf("gate activation status %q is invalid", a.Status)
	}
	if a.Status == StatusOpen && (a.DecisionEventID != "" || a.SupersededReason != "") {
		return fmt.Errorf("open gate activation carries terminal evidence")
	}
	if a.Status == StatusDecisionCommitted && strings.TrimSpace(a.DecisionEventID) == "" {
		return fmt.Errorf("decision-committed gate activation requires decision event id")
	}
	if a.Status == StatusRouted && strings.TrimSpace(a.DecisionEventID) == "" {
		return fmt.Errorf("routed gate activation requires decision event id")
	}
	if a.Status == StatusSuperseded && strings.TrimSpace(a.SupersededReason) == "" {
		return fmt.Errorf("superseded gate activation requires reason")
	}
	return nil
}

func (a Activation) Key() string {
	parts := []string{strings.TrimSpace(a.FlowID), strings.TrimSpace(a.DecisionID)}
	for i := range parts {
		parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
	}
	return strings.Join(parts, ".")
}

func (a *Activation) CommitDecision(eventID string, now time.Time) error {
	if a == nil || a.Status != StatusOpen {
		return fmt.Errorf("gate activation is not open")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("gate decision event id is required")
	}
	a.Status = StatusDecisionCommitted
	a.DecisionEventID = eventID
	a.UpdatedAt = normalizedNow(now)
	return a.Validate()
}

func (a *Activation) Route(eventID string, now time.Time) error {
	if a == nil || a.Status != StatusDecisionCommitted || strings.TrimSpace(a.DecisionEventID) != strings.TrimSpace(eventID) {
		return fmt.Errorf("gate decision event is not authoritative")
	}
	a.Status = StatusRouted
	a.UpdatedAt = normalizedNow(now)
	return a.Validate()
}

func (a *Activation) Supersede(reason string, now time.Time) bool {
	if a == nil || a.Status != StatusOpen {
		return false
	}
	a.Status = StatusSuperseded
	a.SupersededReason = strings.TrimSpace(reason)
	a.UpdatedAt = normalizedNow(now)
	return a.Validate() == nil
}

func Store(buckets map[string]map[string]any, activation Activation) error {
	if buckets == nil {
		return fmt.Errorf("gate state buckets are nil")
	}
	if err := activation.Validate(); err != nil {
		return err
	}
	bucket := buckets[BucketKey]
	if bucket == nil {
		bucket = map[string]any{}
		buckets[BucketKey] = bucket
	}
	raw, err := json.Marshal(activation)
	if err != nil {
		return err
	}
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil {
		return err
	}
	bucket[activation.Key()] = stored
	return nil
}

func Load(buckets map[string]map[string]any, flowID, decisionID string) (Activation, bool, error) {
	probe := Activation{FlowID: strings.TrimSpace(flowID), DecisionID: strings.TrimSpace(decisionID)}
	if buckets == nil || buckets[BucketKey] == nil {
		return Activation{}, false, nil
	}
	value, ok := buckets[BucketKey][probe.Key()]
	if !ok {
		return Activation{}, false, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return Activation{}, false, err
	}
	var activation Activation
	if err := json.Unmarshal(raw, &activation); err != nil {
		return Activation{}, false, err
	}
	if err := activation.Validate(); err != nil {
		return Activation{}, false, err
	}
	return activation, true, nil
}

func List(buckets map[string]map[string]any) ([]Activation, error) {
	if buckets == nil || buckets[BucketKey] == nil {
		return nil, nil
	}
	out := make([]Activation, 0, len(buckets[BucketKey]))
	for _, value := range buckets[BucketKey] {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		var activation Activation
		if err := json.Unmarshal(raw, &activation); err != nil {
			return nil, err
		}
		if err := activation.Validate(); err != nil {
			return nil, err
		}
		out = append(out, activation)
	}
	return out, nil
}

func normalizedNow(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}
