package bus

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

// IsSubmittedInboundEvent identifies only the exact authenticated batch event,
// so its rejecting gateway can own the payload diagnostic without duplication.
func IsSubmittedInboundEvent(ctx context.Context, event events.Event) bool {
	_, submitted := authenticatedProviderPublicationForEvent(ctx, event)
	return submitted
}

// Only the submitted event's admission step may mint this boundary. Generated
// publications and later planning failures do not become caller input errors.
type submittedInboundValidation struct{ cause error }

func (e *submittedInboundValidation) Error() string { return e.cause.Error() }
func (e *submittedInboundValidation) Unwrap() error { return e.cause }

func markSubmittedInboundValidation(err error) error {
	if errors.Is(err, ErrPayloadValidation) {
		return &submittedInboundValidation{cause: err}
	}
	return err
}

// SubmittedInboundValidationPath returns untrusted display data, not a message.
// Every independent cause must be validation-only; one cleanup/store failure
// keeps the entire failure transient, even under nested wrapping and joins.
func SubmittedInboundValidationPath(err error) (string, bool) {
	violation, onlyValidation := inboundValidationCauses(err, false)
	if !onlyValidation || violation == nil {
		return "", false
	}
	return violation.Path, true
}

func inboundValidationCauses(err error, submitted bool) (*eventschema.Violation, bool) {
	switch err := err.(type) {
	case *submittedInboundValidation:
		return inboundValidationCauses(err.cause, true)
	case *eventschema.Violation:
		return err, submitted && err != nil
	case interface{ Unwrap() []error }:
		var first *eventschema.Violation
		causes := err.Unwrap()
		if len(causes) == 0 {
			return nil, false
		}
		for _, cause := range causes {
			violation, valid := inboundValidationCauses(cause, submitted)
			if !valid {
				return nil, false
			}
			if first == nil {
				first = violation
			}
		}
		return first, true
	case interface{ Unwrap() error }:
		return inboundValidationCauses(err.Unwrap(), submitted)
	default:
		return nil, submitted && err == ErrPayloadValidation
	}
}
