package pipeline

import (
	"fmt"
	"math"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
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

// A request correlates a possibly lost admission acknowledgment. It cannot
// authorize topology execution; only the selected store's exact receipt can.
type DynamicFlowRuntimeActivationRequest struct {
	id          string
	planJSON    []byte
	planHash    string
	planError   error
	predecessor uint64
	disposition string
	binding     runtimeprocessbinding.Binding
}

func NewDynamicFlowRuntimeActivationRequest(plan DynamicFlowRuntimeReadinessPlan, predecessor uint64, disposition string, binding runtimeprocessbinding.Binding) DynamicFlowRuntimeActivationRequest {
	request := DynamicFlowRuntimeActivationRequest{id: uuid.NewString(), predecessor: predecessor, disposition: disposition, binding: binding}
	normalized, err := plan.Normalized()
	if err != nil {
		request.planError = err
		return request
	}
	request.planJSON, request.planError = canonicaljson.MarshalPreservingNumberKinds(normalized)
	request.planHash = canonicaljson.HashBytes(request.planJSON)
	return request
}

func (r DynamicFlowRuntimeActivationRequest) Validate() error {
	if id, err := uuid.Parse(r.id); err != nil || id == uuid.Nil || id.String() != r.id {
		return fmt.Errorf("flow activation request requires an exact nonzero request id")
	}
	if r.predecessor == 0 || r.predecessor > math.MaxInt64 || (r.disposition != "planned" && r.predecessor == math.MaxInt64) {
		return fmt.Errorf("flow activation request requires an observed predecessor")
	}
	switch r.disposition {
	case "planned", "accepted", "superseded", "retired", "aborted":
	default:
		return fmt.Errorf("flow activation request requires its exact observed disposition")
	}
	if err := r.binding.Validate(); err != nil {
		return err
	}
	if r.planError != nil {
		return r.planError
	}
	plan, err := DecodeFlowReadinessPlan(r.planJSON, r.planHash)
	if err != nil {
		return err
	}
	if plan.BundleHash != r.binding.BundleHash {
		return fmt.Errorf("flow activation request differs from its generation source")
	}
	return nil
}

func (r DynamicFlowRuntimeActivationRequest) ID() string { return r.id }
func (r DynamicFlowRuntimeActivationRequest) Plan() DynamicFlowRuntimeReadinessPlan {
	plan, _ := DecodeFlowReadinessPlan(r.planJSON, r.planHash)
	return plan
}
func (r DynamicFlowRuntimeActivationRequest) Predecessor() uint64            { return r.predecessor }
func (r DynamicFlowRuntimeActivationRequest) PredecessorDisposition() string { return r.disposition }
func (r DynamicFlowRuntimeActivationRequest) AttemptOrdinal() uint64 {
	if r.disposition == "planned" {
		return r.predecessor
	}
	return r.predecessor + 1
}
func (r DynamicFlowRuntimeActivationRequest) ProcessBinding() runtimeprocessbinding.Binding {
	return r.binding
}

func (r DynamicFlowRuntimeActivationRequest) ValidateResolution(resolved DynamicFlowRuntimeActivationResolution) error {
	switch resolved.Disposition {
	case FlowActivationUnresolved, FlowActivationUnadmitted, FlowActivationForeign:
		if resolved.Attempt != (DynamicFlowRuntimeActivationAttempt{}) {
			return fmt.Errorf("non-admitted activation resolution returned execution authority")
		}
	case FlowActivationAdmitted:
		if err := resolved.Attempt.Validate(); err != nil {
			return err
		}
		plan := r.Plan()
		if resolved.Attempt.RunID() != plan.RunID || resolved.Attempt.InstancePath() != plan.Identity.InstancePath || resolved.Attempt.Ordinal() != r.AttemptOrdinal() || !resolved.Attempt.ProcessBinding().Equal(r.binding) {
			return fmt.Errorf("activation resolution differs from its exact retained request")
		}
	default:
		return fmt.Errorf("invalid activation resolution disposition")
	}
	return nil
}

type FlowActivationResolution uint8

const (
	FlowActivationUnresolved FlowActivationResolution = iota
	FlowActivationUnadmitted
	FlowActivationAdmitted
	FlowActivationForeign
)

type DynamicFlowRuntimeActivationResolution struct {
	Disposition FlowActivationResolution
	Attempt     DynamicFlowRuntimeActivationAttempt
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
