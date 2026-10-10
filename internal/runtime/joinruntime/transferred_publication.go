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
	EventID        string             `json:"event_id"`
	SourceRunID    string             `json:"source_run_id"`
	SourceEventID  string             `json:"source_event_id"`
	AuthorityStamp string             `json:"authority_stamp"`
	ExecutionMode  executionmode.Mode `json:"execution_mode"`
}

func (p TransferredPublication) Validate(ref timeridentity.JoinRef) error {
	entry := ref.StageEntry()
	if !ref.Valid() || ref.Mode() != timeridentity.JoinRefModeArrival || entry.Empty() || entry.FlowScope != ref.FlowPath() {
		return fmt.Errorf("transferred publication requires its exact arrival stage entry")
	}
	for _, value := range []string{entry.RunID, p.EventID, p.SourceRunID, p.SourceEventID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("transferred publication requires canonical nonzero run and event UUIDs")
		}
	}
	if p.SourceRunID == entry.RunID || p.EventID != activityidentity.ForkLineageEventID(entry.RunID, p.SourceEventID) {
		return fmt.Errorf("transferred publication requires distinct source run and deterministic destination event")
	}
	if p.AuthorityStamp == "" || p.AuthorityStamp != strings.TrimSpace(p.AuthorityStamp) || !p.ExecutionMode.Valid() {
		return fmt.Errorf("transferred publication requires exact selection authority and execution mode")
	}
	return nil
}
