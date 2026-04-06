package contracts

import (
	"errors"
	"strings"
)

var (
	ErrLoadValidation              = errors.New("contracts: bundle load validation failed")
	ErrInvalidField                = errors.New("contracts: invalid field")
	ErrConflictingCompletion       = errors.New("contracts: handler declares both on_complete and rules")
	ErrDeprecatedGuardFallback     = errors.New("contracts: deprecated id-only guard")
	ErrMultipleAuthoritativeOwners = errors.New("contracts: multiple authoritative system node owners")
)

type LoadValidationError struct {
	Items []error
}

func (e *LoadValidationError) Error() string {
	if e == nil || len(e.Items) == 0 {
		return ErrLoadValidation.Error()
	}
	lines := make([]string, 0, len(e.Items))
	for _, item := range e.Items {
		if item == nil {
			continue
		}
		lines = append(lines, strings.TrimSpace(item.Error()))
	}
	if len(lines) == 0 {
		return ErrLoadValidation.Error()
	}
	return ErrLoadValidation.Error() + ":\n- " + strings.Join(lines, "\n- ")
}

func (e *LoadValidationError) Is(target error) bool {
	if target == nil {
		return false
	}
	if target == ErrLoadValidation {
		return true
	}
	for _, item := range e.Items {
		if errors.Is(item, target) {
			return true
		}
	}
	return false
}
