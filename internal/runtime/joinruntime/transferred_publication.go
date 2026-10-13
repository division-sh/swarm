package joinruntime

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

// TransferredPublication retains projected publication coordinates, not proof
// that the destination event exists or authority to schedule or deliver it.
type TransferredPublication struct {
	EventID        string                    `json:"event_id"`
	SourceRunID    string                    `json:"source_run_id"`
	SourceEventID  string                    `json:"source_event_id"`
	AuthorityStamp string                    `json:"authority_stamp"`
	ExecutionMode  executionmode.Mode        `json:"execution_mode"`
	CommittedCause CommittedPublicationCause `json:"committed_cause"`
}

// CommittedPublicationCause names retained evidence only. The immediate source
// remains SourceRunID/SourceEventID, even when that pending event never existed.
type CommittedPublicationCause struct {
	RunID   string `json:"run_id"`
	EventID string `json:"event_id"`
}

func (p TransferredPublication) Validate(runID string, ref timeridentity.JoinRef) error {
	entry := ref.StageEntry()
	if !ref.Valid() {
		return fmt.Errorf("transferred publication requires its exact join owner")
	}
	switch ref.Mode() {
	case timeridentity.JoinRefModeArrival:
		if entry.Empty() || entry.RunID != runID || entry.FlowScope != ref.FlowPath() {
			return fmt.Errorf("transferred publication requires its exact arrival stage entry")
		}
	case timeridentity.JoinRefModeFanOutDelivery:
		intent, valid := ref.FanOutDelivery()
		if !valid || intent.TriggeringDeliveryID() == "" {
			return fmt.Errorf("transferred publication requires its exact fan-out intent")
		}
	default:
		return fmt.Errorf("transferred publication join kind is unsupported")
	}
	for _, value := range []string{runID, p.EventID, p.SourceRunID, p.SourceEventID, p.CommittedCause.RunID, p.CommittedCause.EventID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("transferred publication requires canonical nonzero run and event UUIDs")
		}
	}
	if p.SourceRunID == runID || p.EventID != activityidentity.ForkLineageEventID(runID, p.SourceEventID) {
		return fmt.Errorf("transferred publication requires distinct source run and deterministic destination event")
	}
	if p.CommittedCause.RunID == runID || (p.CommittedCause.RunID == p.SourceRunID && p.CommittedCause.EventID != p.SourceEventID) {
		return fmt.Errorf("transferred publication contradicts its retained committed cause")
	}
	if p.AuthorityStamp == "" || p.AuthorityStamp != strings.TrimSpace(p.AuthorityStamp) || !p.ExecutionMode.Valid() {
		return fmt.Errorf("transferred publication requires exact selection authority and execution mode")
	}
	return nil
}
