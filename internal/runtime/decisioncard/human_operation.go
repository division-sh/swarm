package decisioncard

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

// HumanTaskOperationID is the printable persistence projection of an exact
// run/logical-call coordinate, not the NUL-delimited internal coordinate itself.
type HumanTaskOperationID struct{ value uuid.UUID }

func NewHumanTaskOperationID(runID, logicalCall string) (HumanTaskOperationID, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(logicalCall) == "" {
		return HumanTaskOperationID{}, fmt.Errorf("human-task operation requires exact run and logical-call identities")
	}
	coordinate, err := canonicaljson.Bytes([]string{runID, logicalCall})
	if err != nil {
		return HumanTaskOperationID{}, err
	}
	return HumanTaskOperationID{value: uuid.NewSHA1(uuid.NameSpaceOID, append([]byte("swarm.human-task.operation.v1\x00"), coordinate...))}, nil
}

func (id HumanTaskOperationID) String() string {
	if id.value == uuid.Nil {
		return ""
	}
	return "human-task-operation:v1:" + id.value.String()
}

func parseHumanTaskOperationID(raw string) (HumanTaskOperationID, error) {
	const prefix = "human-task-operation:v1:"
	if !strings.HasPrefix(raw, prefix) {
		return HumanTaskOperationID{}, fmt.Errorf("human-task operation requires its canonical printable identity")
	}
	value, err := uuid.Parse(strings.TrimPrefix(raw, prefix))
	id := HumanTaskOperationID{value: value}
	if err != nil || value == uuid.Nil || value.Version() != 5 || raw != id.String() {
		return HumanTaskOperationID{}, fmt.Errorf("invalid canonical human-task operation identity")
	}
	return id, nil
}
