package timeridentity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// StageEntryRef is lifecycle evidence, not a business partition or a clock.
// The lifecycle owner admits local entries when construction or a transition
// commits. Fork origins retain that source history, not a child occurrence.
type StageEntryRef struct {
	RunID        string `json:"run_id"`
	OriginRunID  string `json:"origin_run_id,omitempty"`
	FlowScope    string `json:"flow_scope"`
	InstanceID   string `json:"instance_id"`
	InstancePath string `json:"instance_path"`
	EntityID     string `json:"entity_id"`
	Stage        string `json:"stage"`
	Cause        string `json:"cause"`
	EventID      string `json:"event_id,omitempty"`
	OccurrenceID string `json:"occurrence_id,omitempty"`
	TransitionID string `json:"transition_id,omitempty"`
}

func (r StageEntryRef) Validate() error {
	if r.RunID == "" || r.RunID != strings.TrimSpace(r.RunID) || r.EntityID == "" || r.EntityID != strings.TrimSpace(r.EntityID) || r.Stage == "" || r.Stage != strings.TrimSpace(r.Stage) {
		return fmt.Errorf("stage entry requires exact run, entity and stage identity")
	}
	if r.OriginRunID != "" && (r.OriginRunID != strings.TrimSpace(r.OriginRunID) || r.OriginRunID == r.RunID) {
		return fmt.Errorf("inherited stage entry requires an exact, distinct origin run")
	}
	if r.InstancePath == "" || r.InstanceID == "" || r.FlowScope != strings.Trim(strings.TrimSpace(r.FlowScope), "/") || r.InstanceID != strings.TrimSpace(r.InstanceID) || r.InstancePath != strings.Trim(strings.TrimSpace(r.InstancePath), "/") {
		return fmt.Errorf("stage entry contains a noncanonical instance owner")
	}
	switch r.Cause {
	case "construction":
		if r.EventID != "" || r.OccurrenceID != "" || r.TransitionID != "" {
			return fmt.Errorf("initial stage entry cannot carry transition evidence")
		}
	case "delivery", "timer", "gate":
		if r.EventID == "" || r.OccurrenceID == "" || r.TransitionID == "" || r.EventID != strings.TrimSpace(r.EventID) || r.OccurrenceID != strings.TrimSpace(r.OccurrenceID) || r.TransitionID != strings.TrimSpace(r.TransitionID) {
			return fmt.Errorf("transition stage entry requires event, admitted occurrence and selected transition evidence")
		}
	default:
		return fmt.Errorf("stage entry cause %q is invalid", r.Cause)
	}
	return nil
}

func (r StageEntryRef) Empty() bool { return r == (StageEntryRef{}) }

func (r StageEntryRef) Key() string {
	if r.Validate() != nil {
		return ""
	}
	raw, _ := json.Marshal(r)
	digest := sha256.Sum256(raw)
	return "entry:" + hex.EncodeToString(digest[:])
}

func (r StageEntryRef) RequireOwner(runID, flowScope, instanceID, instancePath, entityID, stage string) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.RunID != runID || r.FlowScope != flowScope || r.InstanceID != instanceID || r.InstancePath != instancePath || r.EntityID != entityID || r.Stage != stage {
		return fmt.Errorf("stage entry disagrees with its lifecycle owner")
	}
	return nil
}

func StageEntryRefFromValue(value any) (StageEntryRef, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return StageEntryRef{}, err
	}
	var ref StageEntryRef
	if err := decodeStrictJSON(raw, &ref); err != nil {
		return StageEntryRef{}, fmt.Errorf("decode stage entry: %w", err)
	}
	return ref, ref.Validate()
}
