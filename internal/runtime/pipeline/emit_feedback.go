package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type EmitStageOrigin string

const (
	EmitStageAcceptance EmitStageOrigin = "acceptance"
	EmitStageHandler    EmitStageOrigin = "handler_commit"
)

// EmitDispatchDisposition describes the publication call, not completion of
// queued receivers or permission to execute them again.
type EmitDispatchDisposition string

const (
	EmitDispatchReturned EmitDispatchDisposition = "dispatch_returned"
	EmitDispatchQueued   EmitDispatchDisposition = "accepted_deferred"
	EmitDispatchError    EmitDispatchDisposition = "post_commit_error"
)

type WorkflowEmitFeedback struct {
	Receipt     pipelineobligation.CommittedStageReceipt
	StageOrigin EmitStageOrigin
	Dispatch    EmitDispatchDisposition
}

func (f WorkflowEmitFeedback) Validate() error {
	if err := f.Receipt.Validate(); err != nil {
		return err
	}
	switch f.StageOrigin {
	case EmitStageAcceptance, EmitStageHandler:
	default:
		return fmt.Errorf("emit feedback requires an explicit stage evidence origin")
	}
	switch f.Dispatch {
	case EmitDispatchReturned, EmitDispatchQueued, EmitDispatchError:
	default:
		return fmt.Errorf("emit feedback requires an explicit dispatch disposition")
	}
	if f.Dispatch == EmitDispatchQueued && f.StageOrigin != EmitStageAcceptance {
		return fmt.Errorf("queued emit feedback cannot claim handler execution")
	}
	return nil
}

type emitFeedbackWire struct {
	Version     int                                      `json:"version"`
	Receipt     pipelineobligation.CommittedStageReceipt `json:"receipt"`
	StageOrigin EmitStageOrigin                          `json:"stage_origin"`
	Dispatch    EmitDispatchDisposition                  `json:"dispatch"`
}

func (f WorkflowEmitFeedback) MarshalJSON() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(emitFeedbackWire{Version: 1, Receipt: f.Receipt, StageOrigin: f.StageOrigin, Dispatch: f.Dispatch})
}

func (f *WorkflowEmitFeedback) UnmarshalJSON(raw []byte) error {
	if f == nil {
		return fmt.Errorf("emit feedback destination is nil")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire emitFeedbackWire
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("emit feedback contains trailing JSON")
	}
	if wire.Version != 1 {
		return fmt.Errorf("unsupported emit feedback version")
	}
	value := WorkflowEmitFeedback{Receipt: wire.Receipt, StageOrigin: wire.StageOrigin, Dispatch: wire.Dispatch}
	if err := value.Validate(); err != nil {
		return err
	}
	*f = value
	return nil
}

type WorkflowEmitFeedbackCommit struct {
	Feedback     WorkflowEmitFeedback
	Acknowledged bool
	Replay       bool
}

// WorkflowEmitFeedbackOwner freezes presentation evidence only. It grants no
// delivery claim, settlement right, or repeat-dispatch permission.
type WorkflowEmitFeedbackOwner interface {
	CommitWorkflowEmitFeedback(context.Context, WorkflowEmitFeedback) (WorkflowEmitFeedbackCommit, error)
	ReadWorkflowEmitFeedback(context.Context, string, flowidentity.RunScopedFlowInstance) (WorkflowEmitFeedback, bool, error)
	ReadWorkflowPublicationStages(context.Context, string, flowidentity.RunScopedFlowInstance) (WorkflowPublicationStageEvidence, bool, error)
}

type WorkflowEmitResult struct {
	Accepted          bool
	FeedbackCommitted bool
	Replay            bool
	Feedback          WorkflowEmitFeedback
}

func (r WorkflowEmitResult) Validate() error {
	if r.FeedbackCommitted {
		if !r.Accepted {
			return fmt.Errorf("emit result cannot acknowledge feedback before publication")
		}
		return r.Feedback.Validate()
	}
	if r.Replay || r.Feedback != (WorkflowEmitFeedback{}) {
		return fmt.Errorf("emit result cannot present unacknowledged feedback")
	}
	return nil
}
