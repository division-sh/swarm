package engine

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

func (e *Executor) computeModuleInputs(frame *executionFrame, plan *contracts.ComputeModuleSpec, module contracts.PolicyModule, moduleID, rowID string) ([]byte, string, error) {
	private := len(plan.SecretInput) > 0
	fail := func(cause error) ([]byte, string, error) {
		return nil, "", computemodule.InvocationError(computemodule.CodeABI, moduleID, rowID, cause, private)
	}
	current := e.currentContext(frame)
	inputs := map[string]any{}
	for name, rawPath := range plan.Input {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		path, ok := plan.InputPaths[name]
		if !ok {
			path = paths.Parse(rawPath)
		}
		if path.IsZero() {
			return fail(fmt.Errorf("input.%s has no mapped source path", name))
		}
		value, exists := current.Lookup(path)
		if !exists {
			return fail(fmt.Errorf("input.%s source %s is missing", name, path.String()))
		}
		inputs[name] = value
	}
	var identity string
	if private {
		var err error
		identity, err = e.injectComputeModuleSecretInputs(frame.ctx, plan.SecretInput, inputs, moduleID, rowID)
		if err != nil {
			return nil, "", err
		}
	}
	if err := eventschema.ValidatePayloadAgainstSchema(module.InputSchema, inputs); err != nil {
		return fail(fmt.Errorf("input schema violation: %w", err))
	}
	raw, err := computemodule.CanonicalJSONBytes(inputs)
	if err != nil {
		return fail(err)
	}
	if !private {
		identity = computemodule.HashBytes(raw)
	}
	return raw, identity, nil
}

func (e *Executor) injectComputeModuleSecretInputs(ctx context.Context, bindings map[string]string, inputs map[string]any, moduleID, rowID string) (string, error) {
	fail := func(cause error) (string, error) {
		return "", computemodule.InvocationError(computemodule.CodeABI, moduleID, rowID, cause, true)
	}
	for name, key := range bindings {
		if _, overlaps := inputs[name]; overlaps || strings.TrimSpace(key) == "" {
			return fail(nil)
		}
	}
	safe, err := computemodule.CanonicalJSONBytes(map[string]any{"input": inputs, "secret_input": bindings})
	if err != nil {
		return fail(err)
	}
	identity := computemodule.HashBytes(append([]byte("compute_module:secret_input:v1\x00"), safe...))
	if e.deps.ComputeSecretInputs == nil {
		return fail(nil)
	}
	values, err := e.deps.ComputeSecretInputs(ctx, bindings)
	if err != nil {
		return fail(err)
	}
	if len(values) != len(bindings) {
		return fail(nil)
	}
	for name := range bindings {
		value, present := values[name]
		if !present || strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
			return fail(nil)
		}
		inputs[name] = value
	}
	return identity, nil
}
