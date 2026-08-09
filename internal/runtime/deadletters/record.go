package deadletters

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type Record struct {
	OriginalEventID string
	DeliveryID      string
	ClaimVersion    int64
	OriginalEvent   string
	OriginalPayload json.RawMessage
	EntityID        string
	FlowInstance    string
	Failure         runtimefailures.Envelope
	RetryCount      int
	ChainDepth      int
	HandlerNode     string
	Timestamp       string
}

type InsertResult struct {
	DeadLetterID string
	Inserted     bool
}

// IdentityConflict reports a duplicate dead-letter identity whose immutable
// facts disagree with the already committed record.
type IdentityConflict struct {
	DeadLetterID string
	Fields       []string
}

func (e *IdentityConflict) Error() string {
	if e == nil {
		return "dead letter identity conflict"
	}
	fields := append([]string(nil), e.Fields...)
	sort.Strings(fields)
	return fmt.Sprintf("dead letter %s identity conflict: %s", strings.TrimSpace(e.DeadLetterID), strings.Join(fields, ", "))
}

func NewIdentityConflict(deadLetterID string, fields []string) error {
	return &IdentityConflict{DeadLetterID: strings.TrimSpace(deadLetterID), Fields: append([]string(nil), fields...)}
}

type Persistence interface {
	RecordDeadLetter(context.Context, Record) error
}
