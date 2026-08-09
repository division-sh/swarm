package apiidempotency

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrConflict = errors.New("api idempotency conflict")

type Request struct {
	Method         string
	ActorTokenID   string
	IdempotencyKey string
	RequestHash    string
	ResourceID     string
	TTL            time.Duration
	Now            time.Time
}

type Completion struct {
	ResourceID string
	Response   json.RawMessage
}

type ConflictError struct {
	OriginalRequestHash    string
	ConflictingRequestHash string
	Method                 string
	ResourceID             string
}

func (e *ConflictError) Error() string { return "api idempotency conflict" }

func (e *ConflictError) Is(target error) bool { return target == ErrConflict }
