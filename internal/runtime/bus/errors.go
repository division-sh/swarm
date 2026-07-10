package bus

import "errors"

var (
	ErrPayloadValidation = errors.New("bus: payload validation failed")
	ErrInvalidEventType  = errors.New("bus: invalid event type")
)
