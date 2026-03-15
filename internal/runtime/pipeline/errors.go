package pipeline

import (
	"errors"
	"strings"
)

var (
	ErrContractBundleNil              = errors.New("pipeline: workflow contract bundle is nil")
	ErrUnknownHandlerAction           = errors.New("pipeline: handler action is not executable")
	ErrGuardEscalateRequiresEventType = errors.New("pipeline: guard on_fail escalate requires event type")
	ErrMissingRequiredAgent           = errors.New("pipeline: required agent missing from merged agents")
	ErrWorkflowValidation             = errors.New("pipeline: workflow contract validation failed")
)

type WorkflowContractValidationError struct {
	Items []error
}

func (e *WorkflowContractValidationError) Error() string {
	if e == nil || len(e.Items) == 0 {
		return ErrWorkflowValidation.Error()
	}
	lines := make([]string, 0, len(e.Items))
	for _, item := range e.Items {
		if item == nil {
			continue
		}
		lines = append(lines, strings.TrimSpace(item.Error()))
	}
	if len(lines) == 0 {
		return ErrWorkflowValidation.Error()
	}
	return ErrWorkflowValidation.Error() + ":\n- " + strings.Join(lines, "\n- ")
}

func (e *WorkflowContractValidationError) Is(target error) bool {
	if target == nil {
		return false
	}
	if target == ErrWorkflowValidation {
		return true
	}
	for _, item := range e.Items {
		if errors.Is(item, target) {
			return true
		}
	}
	return false
}
