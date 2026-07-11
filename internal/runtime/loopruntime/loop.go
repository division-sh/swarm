package loopruntime

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/google/uuid"
)

const BucketKey = "handler_loops"

type Status string

const (
	StatusOpen   Status = "open"
	StatusClosed Status = "closed"
)

type CloseReason string

const (
	CloseReasonCompleted CloseReason = "completed"
	CloseReasonEscaped   CloseReason = "escaped"
)

type Activation struct {
	FlowID         string      `json:"flow_id,omitempty"`
	LoopID         string      `json:"loop_id"`
	RevisionField  string      `json:"revision_field"`
	ActivationID   string      `json:"activation_id"`
	RevisionID     string      `json:"revision_id"`
	Attempt        int         `json:"attempt"`
	MaxAttempts    int         `json:"max_attempts"`
	CurrentStage   string      `json:"current_stage"`
	Status         Status      `json:"status"`
	CloseReason    CloseReason `json:"close_reason,omitempty"`
	StartedByEvent string      `json:"started_by_event"`
	UpdatedByEvent string      `json:"updated_by_event"`
	StartedAt      time.Time   `json:"started_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

type AdmissionDisposition string

const (
	AdmissionAccepted   AdmissionDisposition = "accepted"
	AdmissionIdempotent AdmissionDisposition = "idempotent"
	AdmissionEarly      AdmissionDisposition = "early"
	AdmissionStale      AdmissionDisposition = "stale"
	AdmissionUnexpected AdmissionDisposition = "unexpected"
)

type Admission struct {
	Disposition AdmissionDisposition
	Activation  Activation
	Escaped     bool
}

func New(runID, entityID, flowID, loopID, revisionField, startEventID, stage string, maxAttempts int, now time.Time) (Activation, error) {
	activationID := deterministicID("activation", runID, entityID, flowID, loopID, startEventID)
	activation := Activation{
		FlowID:         strings.TrimSpace(flowID),
		LoopID:         strings.TrimSpace(loopID),
		RevisionField:  strings.TrimSpace(revisionField),
		ActivationID:   activationID,
		RevisionID:     revisionID(activationID, 1),
		Attempt:        1,
		MaxAttempts:    maxAttempts,
		CurrentStage:   strings.TrimSpace(stage),
		Status:         StatusOpen,
		StartedByEvent: strings.TrimSpace(startEventID),
		UpdatedByEvent: strings.TrimSpace(startEventID),
		StartedAt:      now.UTC(),
		UpdatedAt:      now.UTC(),
	}
	if err := activation.Validate(); err != nil {
		return Activation{}, err
	}
	return activation, nil
}

func (a Activation) Validate() error {
	if strings.TrimSpace(a.LoopID) == "" || strings.TrimSpace(a.RevisionField) == "" || strings.TrimSpace(a.ActivationID) == "" || strings.TrimSpace(a.RevisionID) == "" {
		return fmt.Errorf("loop activation identity is incomplete")
	}
	if a.Attempt <= 0 || a.MaxAttempts <= 0 || a.Attempt > a.MaxAttempts {
		return fmt.Errorf("loop %s attempt bounds are invalid: attempt=%d max=%d", a.LoopID, a.Attempt, a.MaxAttempts)
	}
	if strings.TrimSpace(a.CurrentStage) == "" {
		return fmt.Errorf("loop %s current stage is required", a.LoopID)
	}
	if a.Status != StatusOpen && a.Status != StatusClosed {
		return fmt.Errorf("loop %s status %q is invalid", a.LoopID, a.Status)
	}
	if a.Status == StatusOpen && a.CloseReason != "" {
		return fmt.Errorf("open loop %s cannot have close reason %q", a.LoopID, a.CloseReason)
	}
	if a.Status == StatusClosed && a.CloseReason == "" {
		return fmt.Errorf("closed loop %s requires a close reason", a.LoopID)
	}
	return nil
}

func (a Activation) Key() string {
	return activationKey(a.FlowID, a.LoopID)
}

func (a Activation) Context() map[string]any {
	return map[string]any{
		"flow_id":        a.FlowID,
		"activation_id":  a.ActivationID,
		"revision_field": a.RevisionField,
		"revision_id":    a.RevisionID,
		"attempt":        a.Attempt,
		"max_attempts":   a.MaxAttempts,
		"id":             a.LoopID,
	}
}

func (a Activation) Generation() attemptgeneration.Generation {
	return attemptgeneration.Generation{
		FlowID: a.FlowID, LoopID: a.LoopID, ActivationID: a.ActivationID,
		RevisionField: a.RevisionField, RevisionID: a.RevisionID, Attempt: a.Attempt,
	}.Normalize()
}

func (a Activation) Admit(revisionID, fromStage string) AdmissionDisposition {
	revisionID = strings.TrimSpace(revisionID)
	if revisionID == "" {
		return AdmissionUnexpected
	}
	if a.Status == StatusClosed {
		if a.ownsRevision(revisionID) {
			return AdmissionStale
		}
		return AdmissionUnexpected
	}
	if revisionID != a.RevisionID {
		if a.ownsRevision(revisionID) {
			return AdmissionStale
		}
		return AdmissionUnexpected
	}
	if strings.TrimSpace(fromStage) != a.CurrentStage {
		return AdmissionEarly
	}
	return AdmissionAccepted
}

func (a Activation) ownsRevision(candidate string) bool {
	candidate = strings.TrimSpace(candidate)
	for attempt := 1; attempt <= a.Attempt; attempt++ {
		if revisionID(a.ActivationID, attempt) == candidate {
			return true
		}
	}
	return false
}

func (a *Activation) AdvanceWithin(stage, eventID string, now time.Time) error {
	if a == nil || a.Status != StatusOpen {
		return fmt.Errorf("loop activation is not open")
	}
	a.CurrentStage = strings.TrimSpace(stage)
	a.UpdatedByEvent = strings.TrimSpace(eventID)
	a.UpdatedAt = now.UTC()
	return a.Validate()
}

func (a *Activation) Repeat(stage, eventID string, now time.Time) (bool, error) {
	if a == nil || a.Status != StatusOpen {
		return false, fmt.Errorf("loop activation is not open")
	}
	a.UpdatedByEvent = strings.TrimSpace(eventID)
	a.UpdatedAt = now.UTC()
	if a.Attempt >= a.MaxAttempts {
		a.Status = StatusClosed
		a.CloseReason = CloseReasonEscaped
		a.CurrentStage = strings.TrimSpace(stage)
		return true, a.Validate()
	}
	a.Attempt++
	a.RevisionID = revisionID(a.ActivationID, a.Attempt)
	a.CurrentStage = strings.TrimSpace(stage)
	return false, a.Validate()
}

func (a *Activation) Close(stage, eventID string, now time.Time) error {
	if a == nil || a.Status != StatusOpen {
		return fmt.Errorf("loop activation is not open")
	}
	a.Status = StatusClosed
	a.CloseReason = CloseReasonCompleted
	a.CurrentStage = strings.TrimSpace(stage)
	a.UpdatedByEvent = strings.TrimSpace(eventID)
	a.UpdatedAt = now.UTC()
	return a.Validate()
}

func Store(buckets map[string]map[string]any, activation Activation) error {
	if buckets == nil {
		return fmt.Errorf("loop state buckets are nil")
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

func Load(buckets map[string]map[string]any, flowID, loopID string) (Activation, bool, error) {
	if buckets == nil || buckets[BucketKey] == nil {
		return Activation{}, false, nil
	}
	value, ok := buckets[BucketKey][activationKey(flowID, loopID)]
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
	keys := make([]string, 0, len(buckets[BucketKey]))
	for key := range buckets[BucketKey] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Activation, 0, len(keys))
	for _, key := range keys {
		value := buckets[BucketKey][key]
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

func PublicStateBuckets(raw map[string]any) map[string]any {
	out := make(map[string]any, len(raw))
	for key, value := range raw {
		if strings.TrimSpace(key) == BucketKey {
			continue
		}
		out[key] = value
	}
	return out
}

type PublicActivation struct {
	ID           string `json:"id"`
	RevisionID   string `json:"revision_id"`
	Attempt      int    `json:"attempt"`
	MaxAttempts  int    `json:"max_attempts"`
	CurrentStage string `json:"current_stage"`
	Status       Status `json:"status"`
	CloseReason  string `json:"close_reason,omitempty"`
}

func PublicActivations(raw map[string]any) ([]PublicActivation, error) {
	bucket, _ := raw[BucketKey].(map[string]any)
	if len(bucket) == 0 {
		return []PublicActivation{}, nil
	}
	keys := make([]string, 0, len(bucket))
	for key := range bucket {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]PublicActivation, 0, len(keys))
	for _, key := range keys {
		encoded, err := json.Marshal(bucket[key])
		if err != nil {
			return nil, err
		}
		var activation Activation
		if err := json.Unmarshal(encoded, &activation); err != nil {
			return nil, err
		}
		if err := activation.Validate(); err != nil {
			return nil, err
		}
		out = append(out, PublicActivation{
			ID: activation.LoopID, RevisionID: activation.RevisionID, Attempt: activation.Attempt,
			MaxAttempts: activation.MaxAttempts, CurrentStage: activation.CurrentStage,
			Status: activation.Status, CloseReason: string(activation.CloseReason),
		})
	}
	return out, nil
}

func Fork(source Activation, forkRunID, entityID string) (Activation, error) {
	activation := source
	activation.ActivationID = deterministicID("fork-activation", forkRunID, entityID, source.FlowID, source.LoopID, source.ActivationID)
	activation.RevisionID = revisionID(activation.ActivationID, activation.Attempt)
	if err := activation.Validate(); err != nil {
		return Activation{}, err
	}
	return activation, nil
}

func ForkGeneration(source attemptgeneration.Generation, forkRunID, entityID string) (attemptgeneration.Generation, error) {
	source = source.Normalize()
	if !source.Valid() {
		return attemptgeneration.Generation{}, fmt.Errorf("source loop generation is invalid")
	}
	activationID := deterministicID("fork-activation", forkRunID, entityID, source.FlowID, source.LoopID, source.ActivationID)
	return attemptgeneration.Generation{
		FlowID: source.FlowID, LoopID: source.LoopID, ActivationID: activationID,
		RevisionField: source.RevisionField, RevisionID: revisionID(activationID, source.Attempt), Attempt: source.Attempt,
	}.Normalize(), nil
}

func activationKey(flowID, loopID string) string {
	encode := func(value string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(value)))
	}
	return encode(flowID) + "." + encode(loopID)
}

func revisionID(activationID string, attempt int) string {
	return deterministicID("revision", activationID, fmt.Sprintf("%d", attempt))
}

func deterministicID(parts ...string) string {
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:workflow-loop:"+strings.Join(parts, "\x00"))).String()
}
