package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/google/uuid"
)

// DynamicFlowRuntimeActivationAttempt is the selected-store receipt for one
// admitted physical topology attempt. The store rechecks this receipt in each
// durable forward-progress mutation; it is not a substitute for that check.
type DynamicFlowRuntimeActivationAttempt struct {
	id           string
	runID        string
	instancePath string
	binding      runtimeprocessbinding.Binding
}

type DynamicFlowRuntimeActivationAdmissionResult struct {
	Attempt      DynamicFlowRuntimeActivationAttempt
	Acknowledged bool
	Reused       bool
}

func NewDynamicFlowRuntimeActivationAttempt(id, runID, instancePath string, binding runtimeprocessbinding.Binding) (DynamicFlowRuntimeActivationAttempt, error) {
	attempt := DynamicFlowRuntimeActivationAttempt{
		id: id, runID: runID, instancePath: instancePath,
		binding: binding,
	}
	return attempt, attempt.Validate()
}

func (a DynamicFlowRuntimeActivationAttempt) Validate() error {
	if _, err := flowidentity.ParseActivationAttemptID(a.id); err != nil {
		return err
	}
	if id, err := uuid.Parse(a.runID); err != nil || id == uuid.Nil || id.String() != a.runID {
		return fmt.Errorf("flow activation attempt requires a canonical nonzero run id")
	}
	if a.instancePath == "" || strings.Trim(a.instancePath, "/ ") != a.instancePath {
		return fmt.Errorf("flow activation attempt requires exact instance")
	}
	return a.binding.Validate()
}

func (a DynamicFlowRuntimeActivationAttempt) ID() string           { return a.id }
func (a DynamicFlowRuntimeActivationAttempt) RunID() string        { return a.runID }
func (a DynamicFlowRuntimeActivationAttempt) InstancePath() string { return a.instancePath }
func (a DynamicFlowRuntimeActivationAttempt) Ordinal() uint64 {
	ordinal, _ := flowidentity.ParseActivationAttemptID(a.id)
	return ordinal
}
func (a DynamicFlowRuntimeActivationAttempt) ProcessBinding() runtimeprocessbinding.Binding {
	return a.binding
}
