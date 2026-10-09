package contracts

import "fmt"

// PolicyModule is the existing runner contract. Its sole declaration owner is
// the scoped tool entry, not a reserved section in user policy values.
type PolicyModule struct {
	Path, Kind, ABI, Entry, Digest, SourcePath, SourceHash string
	Runtime                                                PolicyModuleRuntime
	InputSchema, OutputSchema                              map[string]any
	Limits                                                 PolicyModuleLimits
}

type PolicyModuleRuntime struct {
	Interpreter, InterpreterDigest, SnapshotDigest, HarnessABI string
}

type PolicyModuleLimits struct {
	Gas         uint64
	MemoryPages uint32
	OutputBytes int
}

func cloneToolModule(module PolicyModule) PolicyModule {
	if module.InputSchema != nil {
		module.InputSchema = cloneEventSchemaValue(module.InputSchema).(map[string]any)
	}
	if module.OutputSchema != nil {
		module.OutputSchema = cloneEventSchemaValue(module.OutputSchema).(map[string]any)
	}
	return module
}

func WithToolModule(module PolicyModule) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		input, err := AdmitToolInputSchemaMap(module.InputSchema)
		if err != nil {
			return err
		}
		output, err := AdmitToolInputSchemaMap(module.OutputSchema)
		if err != nil {
			return err
		}
		copyValue := cloneToolModule(module)
		copyValue.InputSchema = input.Projection()
		copyValue.OutputSchema = output.Projection()
		draft.value.module = &copyValue
		draft.value.inputSchema = input
		draft.value.outputSchema = output
		return nil
	})
}

func (e ToolSchemaEntry) Module() (PolicyModule, bool) {
	if e.value == nil || e.value.module == nil {
		return PolicyModule{}, false
	}
	return cloneToolModule(*e.value.module), true
}

func (e ToolSchemaEntry) AgentExposable() bool {
	return e.Handler() != ToolHandlerWasm && e.Handler() != ToolHandlerPython && e.Handler() != ToolHandlerInProcess
}

func (e ToolSchemaEntry) validateModule() error {
	module, present := e.Module()
	if e.Handler() != ToolHandlerWasm && e.Handler() != ToolHandlerPython {
		if present {
			return fmt.Errorf("module requires handler_type wasm or python")
		}
		return nil
	}
	if !present || module.Kind != e.Handler().String() {
		return fmt.Errorf("module contract must match handler_type")
	}
	if module.Path == "" || module.ABI == "" || module.Entry == "" || !computeModuleDigestPattern.MatchString(module.Digest) || module.Limits.Gas == 0 || module.Limits.MemoryPages == 0 || module.Limits.OutputBytes <= 0 {
		return fmt.Errorf("module requires path, ABI, entry, pinned digest and positive limits")
	}
	if e.value.category != ToolCategoryUnspecified || e.value.hasHTTP || e.value.hasMCP || e.value.hasResponseMapping || e.value.hasResponseSuccess || e.value.hasManagedCredential || e.value.hasCompiledResult || len(e.value.credentials) > 0 || e.value.permission.String() != "" || e.value.effect != "" || e.RatePolicy().Enabled() {
		return fmt.Errorf("module tools cannot acquire agent/transport/effect authority")
	}
	for _, pair := range []struct {
		raw    map[string]any
		schema ToolInputSchema
	}{{module.InputSchema, e.InputSchema()}, {module.OutputSchema, e.OutputSchema()}} {
		admitted, err := AdmitToolInputSchemaMap(pair.raw)
		if err != nil {
			return err
		}
		if !admitted.Equal(pair.schema) || admitted.Kind() != ToolSchemaObject || len(admitted.PropertyNames()) == 0 {
			return fmt.Errorf("module schemas require matching nonempty object properties")
		}
		if errs := validateComputeModuleSchema("module schema", pair.raw); len(errs) > 0 {
			return errs[0]
		}
	}
	return nil
}

func (m PolicyModule) declarationValue() map[string]any {
	out := map[string]any{
		"handler_type": m.Kind, "path": m.Path, "abi": m.ABI, "entry": m.Entry, "digest": m.Digest,
		"input_schema": m.InputSchema, "output_schema": m.OutputSchema,
		"limits": map[string]any{"gas": m.Limits.Gas, "memory_pages": m.Limits.MemoryPages, "output_bytes": m.Limits.OutputBytes},
	}
	if m.SourcePath != "" {
		out["source_path"] = m.SourcePath
	}
	if m.SourceHash != "" {
		out["source_hash"] = m.SourceHash
	}
	runtime := map[string]string{}
	for key, value := range map[string]string{"interpreter": m.Runtime.Interpreter, "interpreter_digest": m.Runtime.InterpreterDigest, "snapshot_digest": m.Runtime.SnapshotDigest, "harness_abi": m.Runtime.HarnessABI} {
		if value != "" {
			runtime[key] = value
		}
	}
	if len(runtime) > 0 {
		out["runtime"] = runtime
	}
	return out
}
