package runstart

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type RootInputSet struct {
	Declared []string
	Routable []string
}

type RootInputValidationReason string

const (
	RootInputNotDeclared RootInputValidationReason = "not_declared_root_input"
	RootInputNotRoutable RootInputValidationReason = "declared_root_input_not_routable"
)

type RootInputValidationError struct {
	EventName string
	Reason    RootInputValidationReason
	Inputs    RootInputSet
}

func (e *RootInputValidationError) Error() string {
	if e == nil {
		return ""
	}
	switch e.Reason {
	case RootInputNotDeclared:
		return fmt.Sprintf("run.start input %q is not declared in root input pins; declared root inputs: %s", e.EventName, formatRootInputDomain(e.Inputs.Declared))
	case RootInputNotRoutable:
		return fmt.Sprintf("run.start input %q is declared but has no runtime route; routable root inputs: %s", e.EventName, formatRootInputDomain(e.Inputs.Routable))
	default:
		return fmt.Sprintf("run.start input %q is invalid", e.EventName)
	}
}

func AsRootInputValidationError(err error) (*RootInputValidationError, bool) {
	if err == nil {
		return nil, false
	}
	var validationErr *RootInputValidationError
	if !errors.As(err, &validationErr) || validationErr == nil {
		return nil, false
	}
	out := *validationErr
	out.EventName = strings.TrimSpace(out.EventName)
	out.Inputs = normalizeRootInputSet(out.Inputs)
	return &out, true
}

func DeriveRootInputSet(source semanticview.Source) (RootInputSet, error) {
	if source == nil {
		return RootInputSet{}, fmt.Errorf("semantic source is required")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return RootInputSet{}, fmt.Errorf("workflow contract bundle is required")
	}
	if bundle.RootSchema == nil {
		return RootInputSet{Declared: []string{}, Routable: []string{}}, nil
	}
	declared := normalizeUnique(bundle.RootSchema.Pins.Inputs.Events)
	routeTable, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		return RootInputSet{}, err
	}
	routable := make([]string, 0, len(declared))
	topology := routingtopology.Build(source)
	for _, eventType := range declared {
		if len(routeTable.Resolve(eventType)) > 0 || rootInputRoutableInTopology(topology, eventType) {
			routable = append(routable, eventType)
		}
	}
	return RootInputSet{Declared: declared, Routable: routable}, nil
}

func ValidateInputEvents(source semanticview.Source, inputEvents []string) (RootInputSet, error) {
	inputEvents = normalizeUnique(inputEvents)
	if len(inputEvents) == 0 {
		return RootInputSet{}, nil
	}
	set, err := DeriveRootInputSet(source)
	if err != nil {
		return set, err
	}
	declared := stringSet(set.Declared)
	routable := stringSet(set.Routable)
	for _, eventType := range inputEvents {
		if _, ok := declared[eventType]; !ok {
			return set, newRootInputValidationError(eventType, RootInputNotDeclared, set)
		}
		if _, ok := routable[eventType]; !ok {
			return set, newRootInputValidationError(eventType, RootInputNotRoutable, set)
		}
	}
	return set, nil
}

func newRootInputValidationError(eventName string, reason RootInputValidationReason, inputs RootInputSet) *RootInputValidationError {
	return &RootInputValidationError{
		EventName: strings.TrimSpace(eventName),
		Reason:    reason,
		Inputs:    normalizeRootInputSet(inputs),
	}
}

func normalizeRootInputSet(inputs RootInputSet) RootInputSet {
	return RootInputSet{
		Declared: normalizeUnique(inputs.Declared),
		Routable: normalizeUnique(inputs.Routable),
	}
}

func formatRootInputDomain(values []string) string {
	values = normalizeUnique(values)
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

func normalizeUnique(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func rootInputRoutableInTopology(topology routingtopology.Topology, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	for _, edge := range topology.Edges {
		if edge.Scope != routingtopology.DeliveryScopeTypedPubSub || edge.Producer.Direction != semanticview.EventEndpointInputPin {
			continue
		}
		if edge.Producer.Event.Authored == eventType || edge.Producer.Event.Local == eventType || edge.Producer.Event.Canonical == eventType {
			return true
		}
	}
	return false
}
