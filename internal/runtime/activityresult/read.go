package activityresult

import (
	"context"
	"encoding/json"
)

type Reader interface {
	LoadRecordedActivityResult(context.Context, Query) (Record, bool, error)
}

type Query struct {
	ActivityID     string
	RequestEventID string
	SuccessEventID string
	FailureEventID string
}

type Record struct {
	EventID   string
	EventType string
	Payload   json.RawMessage
}
