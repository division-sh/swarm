package deadletters

import (
	"context"
	"encoding/json"

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

type Persistence interface {
	RecordDeadLetter(context.Context, Record) error
}
