package pipelineobligation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

type stageReceiptWire struct {
	Version      int       `json:"version"`
	EventID      string    `json:"event_id"`
	RunID        string    `json:"run_id"`
	FlowScope    string    `json:"flow_scope"`
	InstanceID   string    `json:"instance_id"`
	InstancePath string    `json:"instance_path"`
	EntityID     string    `json:"entity_id"`
	Stage        string    `json:"stage"`
	StageDefined *bool     `json:"stage_defined"`
	Revision     int64     `json:"revision"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (r CommittedStageReceipt) Validate() error {
	id, err := uuid.Parse(r.eventID)
	if err != nil || id == uuid.Nil || id.String() != r.eventID {
		return fmt.Errorf("committed stage receipt requires its canonical event UUID")
	}
	return r.stage.Validate()
}

func (r CommittedStageReceipt) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	s := r.stage
	return json.Marshal(stageReceiptWire{Version: 1, EventID: r.eventID, RunID: s.Instance.RunID,
		FlowScope: s.Instance.Route.ScopeKey, InstanceID: s.Instance.Route.InstanceID, InstancePath: s.Instance.Route.InstancePath,
		EntityID: s.EntityID, Stage: s.Stage, StageDefined: &s.StageDefined, Revision: s.Revision, UpdatedAt: s.UpdatedAt.UTC()})
}

func (r *CommittedStageReceipt) UnmarshalJSON(raw []byte) error {
	if r == nil {
		return fmt.Errorf("committed stage receipt destination is nil")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire stageReceiptWire
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("committed stage receipt contains trailing JSON")
	}
	if wire.Version != 1 {
		return fmt.Errorf("unsupported committed stage receipt version")
	}
	if wire.StageDefined == nil {
		return fmt.Errorf("committed stage receipt requires explicit stage posture")
	}
	value := CommittedStageReceipt{eventID: wire.EventID, stage: engine.CommittedStage{
		Instance: flowidentity.RunScopedFlowInstance{RunID: wire.RunID, Route: flowidentity.Route{
			ScopeKey: wire.FlowScope, InstanceID: wire.InstanceID, InstancePath: wire.InstancePath,
		}}, EntityID: wire.EntityID, Stage: wire.Stage, StageDefined: *wire.StageDefined, Revision: wire.Revision, UpdatedAt: wire.UpdatedAt.UTC(),
	}}
	if err := value.Validate(); err != nil {
		return err
	}
	*r = value
	return nil
}
