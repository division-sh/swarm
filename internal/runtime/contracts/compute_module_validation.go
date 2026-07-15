package contracts

import (
	stdcontext "context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
)

var computeModuleDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

const (
	policyModuleKindWasm   = "wasm"
	policyModuleKindPython = pythonmodule.Kind
	policyModuleWasmABI    = "core-json-v1"
)

func validateWorkflowComputeModuleContracts(bundle *WorkflowContractBundle) []error {
	if bundle == nil {
		return nil
	}
	errs := []error{}
	errs = append(errs, validateProjectPolicyModulesUnsupported(bundle)...)
	errs = append(errs, validateFlowPolicyModules(bundle)...)
	errs = append(errs, validatePolicySheetComputeModuleRows(bundle)...)
	return errs
}

func validateProjectPolicyModulesUnsupported(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, view := range bundle.ProjectViews() {
		if len(view.Policy.Modules) == 0 {
			continue
		}
		errs = append(errs, fmt.Errorf("%w: project policy %s declares modules; modules must be declared in flow policy.yaml", ErrInvalidField, strings.TrimSpace(view.Paths.Key)))
	}
	return errs
}

func validateFlowPolicyModules(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for _, flowID := range sortedFlowSchemaIDs(bundle.FlowSchemas) {
		view, ok := bundle.FlowViewByID(flowID)
		if !ok || view == nil {
			continue
		}
		for _, moduleID := range sortedPolicyModuleNames(view.Policy.Modules) {
			context := "flow " + flowID + " policy.modules." + moduleID
			errs = append(errs, validatePolicyModuleDeclaration(bundle, context, moduleID, view.Policy.Modules[moduleID])...)
		}
	}
	return errs
}

func validatePolicyModuleDeclaration(bundle *WorkflowContractBundle, context, moduleID string, module PolicyModule) []error {
	errs := []error{}
	if !isPolicySheetPathSegment(moduleID) {
		errs = append(errs, fmt.Errorf("%w: %s module id must be a short name", ErrInvalidField, context))
	}
	if strings.TrimSpace(module.Path) == "" {
		errs = append(errs, fmt.Errorf("%w: %s path is required", ErrInvalidField, context))
	}
	kind := policyModuleKind(module)
	if digest := strings.TrimSpace(module.Digest); !computeModuleDigestPattern.MatchString(digest) {
		errs = append(errs, fmt.Errorf("%w: %s digest must be sha256:<64 lowercase hex>", ErrInvalidField, context))
	}
	if module.Limits.Gas == 0 {
		errs = append(errs, fmt.Errorf("%w: %s limits.gas is required", ErrInvalidField, context))
	}
	if module.Limits.MemoryPages == 0 {
		errs = append(errs, fmt.Errorf("%w: %s limits.memory_pages is required", ErrInvalidField, context))
	}
	if module.Limits.OutputBytes <= 0 {
		errs = append(errs, fmt.Errorf("%w: %s limits.output_bytes is required", ErrInvalidField, context))
	}
	if len(module.InputSchema) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s input_schema is required", ErrInvalidField, context))
	} else {
		errs = append(errs, validateComputeModuleSchema(context+".input_schema", module.InputSchema)...)
	}
	if len(module.OutputSchema) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s output_schema is required", ErrInvalidField, context))
	} else {
		errs = append(errs, validateComputeModuleSchema(context+".output_schema", module.OutputSchema)...)
	}
	raw, _, readErr := PolicyModuleBytes(bundle, module)
	if readErr != nil {
		errs = append(errs, fmt.Errorf("%w: %s %v", ErrInvalidField, context, readErr))
		return errs
	}
	switch kind {
	case policyModuleKindWasm:
		errs = append(errs, validateWasmPolicyModuleDeclaration(context, module, raw)...)
	case policyModuleKindPython:
		errs = append(errs, validatePythonPolicyModuleDeclaration(context, moduleID, module, raw)...)
	default:
		errs = append(errs, fmt.Errorf("%w: %s kind must be wasm or python, got %q", ErrInvalidField, context, strings.TrimSpace(module.Kind)))
	}
	return errs
}

func validateWasmPolicyModuleDeclaration(context string, module PolicyModule, raw []byte) []error {
	errs := []error{}
	if abi := strings.TrimSpace(module.ABI); abi != policyModuleWasmABI {
		errs = append(errs, fmt.Errorf("%w: %s abi must be %s for wasm modules, got %q", ErrInvalidField, context, policyModuleWasmABI, abi))
	}
	if entry := strings.TrimSpace(module.Entry); entry != "compute" {
		errs = append(errs, fmt.Errorf("%w: %s entry must be compute for wasm modules, got %q", ErrInvalidField, context, entry))
	}
	if len(raw) < 4 || string(raw[:4]) != "\x00asm" {
		errs = append(errs, fmt.Errorf("%w: %s path must point to a WebAssembly module", ErrInvalidField, context))
	}
	sum := sha256.Sum256(raw)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	if strings.TrimSpace(module.Digest) != "" && strings.TrimSpace(module.Digest) != actual {
		errs = append(errs, fmt.Errorf("%w: %s digest %s does not match module bytes %s", ErrInvalidField, context, strings.TrimSpace(module.Digest), actual))
	}
	return errs
}

func validatePythonPolicyModuleDeclaration(context, moduleID string, module PolicyModule, raw []byte) []error {
	errs := []error{}
	if abi := strings.TrimSpace(module.ABI); abi != pythonmodule.ABI {
		errs = append(errs, fmt.Errorf("%w: %s abi must be %s for python modules, got %q", ErrInvalidField, context, pythonmodule.ABI, abi))
	}
	if entry := strings.TrimSpace(module.Entry); entry != pythonmodule.DefaultEntry {
		errs = append(errs, fmt.Errorf("%w: %s entry must be %s for python modules, got %q", ErrInvalidField, context, pythonmodule.DefaultEntry, entry))
	}
	if strings.TrimSpace(module.SourcePath) != "" || strings.TrimSpace(module.SourceHash) != "" {
		errs = append(errs, fmt.Errorf("%w: %s python modules use path/digest as source authority; source_path/source_hash are invalid", ErrInvalidField, context))
	}
	identity := pythonmodule.RuntimeIdentity()
	if runtime := module.Runtime; strings.TrimSpace(runtime.Interpreter) != "" ||
		strings.TrimSpace(runtime.InterpreterDigest) != "" ||
		strings.TrimSpace(runtime.SnapshotDigest) != "" ||
		strings.TrimSpace(runtime.HarnessABI) != "" {
		if strings.TrimSpace(runtime.Interpreter) != identity.Interpreter {
			errs = append(errs, fmt.Errorf("%w: %s runtime.interpreter must be %s", ErrInvalidField, context, identity.Interpreter))
		}
		if strings.TrimSpace(runtime.InterpreterDigest) != identity.InterpreterDigest {
			errs = append(errs, fmt.Errorf("%w: %s runtime.interpreter_digest must be %s", ErrInvalidField, context, identity.InterpreterDigest))
		}
		if strings.TrimSpace(runtime.SnapshotDigest) != identity.SnapshotDigest {
			errs = append(errs, fmt.Errorf("%w: %s runtime.snapshot_digest must be %s", ErrInvalidField, context, identity.SnapshotDigest))
		}
		if strings.TrimSpace(runtime.HarnessABI) != identity.HarnessABI {
			errs = append(errs, fmt.Errorf("%w: %s runtime.harness_abi must be %s", ErrInvalidField, context, identity.HarnessABI))
		}
	}
	sum := sha256.Sum256(raw)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	if strings.TrimSpace(module.Digest) != "" && strings.TrimSpace(module.Digest) != actual {
		errs = append(errs, fmt.Errorf("%w: %s digest %s does not match python source bytes %s", ErrInvalidField, context, strings.TrimSpace(module.Digest), actual))
	}
	if len(errs) > 0 {
		return errs
	}
	if err := pythonmodule.ValidateSource(stdcontext.Background(), pythonmodule.Request{
		ModuleID:    moduleID,
		RowID:       "policy.modules." + moduleID,
		Digest:      strings.TrimSpace(module.Digest),
		Entry:       strings.TrimSpace(module.Entry),
		Source:      raw,
		Fuel:        module.Limits.Gas,
		MemoryPages: module.Limits.MemoryPages,
		OutputBytes: module.Limits.OutputBytes,
	}); err != nil {
		errs = append(errs, fmt.Errorf("%w: %s %v", ErrInvalidField, context, err))
	}
	return errs
}

func policyModuleKind(module PolicyModule) string {
	kind := strings.TrimSpace(module.Kind)
	if kind == "" {
		return policyModuleKindWasm
	}
	return kind
}

func validateComputeModuleSchema(context string, schema map[string]any) []error {
	errs := []error{}
	if kind := strings.TrimSpace(fmt.Sprint(schema["type"])); kind != "object" {
		errs = append(errs, fmt.Errorf("%w: %s type must be object", ErrInvalidField, context))
	}
	if len(computeModuleSchemaProperties(schema)) == 0 {
		errs = append(errs, fmt.Errorf("%w: %s properties must declare at least one field", ErrInvalidField, context))
	}
	errs = append(errs, validateComputeModuleSchemaNoFloat(context, schema)...)
	return errs
}

func validateComputeModuleSchemaNoFloat(context string, value any) []error {
	errs := []error{}
	switch typed := value.(type) {
	case map[string]any:
		if raw, ok := typed["type"]; ok {
			switch strings.ToLower(strings.TrimSpace(fmt.Sprint(raw))) {
			case "number", "float", "double":
				errs = append(errs, fmt.Errorf("%w: %s uses float/number type %q; PR1 compute_module schemas support integer, string, bool, object, and array only", ErrInvalidField, context, strings.TrimSpace(fmt.Sprint(raw))))
			}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			errs = append(errs, validateComputeModuleSchemaNoFloat(context+"."+key, typed[key])...)
		}
	case []any:
		for idx, item := range typed {
			errs = append(errs, validateComputeModuleSchemaNoFloat(fmt.Sprintf("%s[%d]", context, idx), item)...)
		}
	}
	return errs
}

func validatePolicySheetComputeModuleRows(bundle *WorkflowContractBundle) []error {
	errs := []error{}
	for nodeID, node := range bundle.Nodes {
		source, _ := bundle.NodeContractSource(nodeID)
		flowID := strings.TrimSpace(source.FlowID)
		policy := bundle.ResolvedPolicyForFlow(flowID)
		for eventType, handler := range node.EventHandlers {
			for idx, rule := range handler.Rules {
				if !policySheetRuleIsComputeModuleValueRow(rule) {
					continue
				}
				context := fmt.Sprintf("node %s handler %s rules[%d] compute_module row %s", strings.TrimSpace(nodeID), strings.TrimSpace(eventType), idx, strings.TrimSpace(rule.ID))
				errs = append(errs, validatePolicySheetComputeModuleRow(context, rule, policy)...)
			}
		}
	}
	return errs
}

func policySheetRuleIsComputeModuleValueRow(rule HandlerRuleEntry) bool {
	if rule.PolicyRow.Kind == PolicySheetRowKindModule {
		return true
	}
	return rule.Compute != nil && rule.Compute.Operation == ComputeOpModule
}

func validatePolicySheetComputeModuleRow(context string, rule HandlerRuleEntry, policy PolicyDocument) []error {
	errs := []error{}
	if rule.PolicyRow.Kind != PolicySheetRowKindModule {
		errs = append(errs, fmt.Errorf("%w: %s compute_module compute must originate from a policy-sheet compute_module row", ErrInvalidField, context))
	}
	if rule.Compute == nil || rule.Compute.Operation != ComputeOpModule || rule.Compute.Module == nil {
		return append(errs, fmt.Errorf("%w: %s compute_module row must lower to compute-owned module operation", ErrInvalidField, context))
	}
	spec := rule.Compute.Module
	target := strings.TrimSpace(spec.StoreTarget())
	if target == "" {
		errs = append(errs, fmt.Errorf("%w: %s compute_module.into is required", ErrInvalidField, context))
	} else if err := validatePolicyModuleStoreTarget(target); err != nil {
		errs = append(errs, fmt.Errorf("%w: %s %v", ErrInvalidField, context, err))
	}
	if computeTarget := strings.TrimSpace(rule.Compute.StoreAs); computeTarget != "" && target != "" && computeTarget != target {
		errs = append(errs, fmt.Errorf("%w: %s compute_module.into %q must match compute.store_as %q", ErrInvalidField, context, target, computeTarget))
	}
	moduleID := strings.TrimSpace(spec.Module)
	module, ok := policy.Modules[moduleID]
	if moduleID == "" {
		errs = append(errs, fmt.Errorf("%w: %s compute_module.module is required", ErrInvalidField, context))
	} else if !ok {
		errs = append(errs, fmt.Errorf("%w: %s compute_module.module %q does not resolve in flow policy.modules", ErrInvalidField, context, moduleID))
	}
	if !ok {
		return errs
	}
	declared := computeModuleSchemaProperties(module.InputSchema)
	required := computeModuleSchemaRequired(module.InputSchema)
	mapped := stringSetFromMap(spec.Input)
	for name := range required {
		if _, ok := mapped[name]; !ok {
			errs = append(errs, fmt.Errorf("%w: %s compute_module.input missing required module input %q", ErrInvalidField, context, name))
		}
	}
	for name := range mapped {
		if _, ok := declared[name]; !ok {
			errs = append(errs, fmt.Errorf("%w: %s compute_module.input maps undeclared module input %q", ErrInvalidField, context, name))
		}
	}
	return errs
}

func validatePolicyModuleStoreTarget(target string) error {
	parsed := paths.Parse(target)
	if parsed.Root != paths.RootComputed || len(parsed.Segments) == 0 {
		return fmt.Errorf("compute_module.into %q must target computed.*", strings.TrimSpace(target))
	}
	for _, segment := range parsed.Segments {
		if !isPolicySheetPathSegment(segment) {
			return fmt.Errorf("compute_module.into %q must be a simple computed.* path", strings.TrimSpace(target))
		}
	}
	return nil
}

func (s ComputeModuleSpec) StoreTarget() string {
	if strings.TrimSpace(s.Into) != "" {
		return strings.TrimSpace(s.Into)
	}
	return ""
}

func sortedPolicyModuleNames(in map[string]PolicyModule) []string {
	names := make([]string, 0, len(in))
	for name := range in {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func computeModuleSchemaProperties(schema map[string]any) map[string]struct{} {
	properties := map[string]struct{}{}
	raw, _ := schema["properties"].(map[string]any)
	for name := range raw {
		name = strings.TrimSpace(name)
		if name != "" {
			properties[name] = struct{}{}
		}
	}
	return properties
}

func computeModuleSchemaRequired(schema map[string]any) map[string]struct{} {
	required := map[string]struct{}{}
	switch raw := schema["required"].(type) {
	case []any:
		for _, value := range raw {
			name := strings.TrimSpace(fmt.Sprint(value))
			if name != "" {
				required[name] = struct{}{}
			}
		}
	case []string:
		for _, value := range raw {
			name := strings.TrimSpace(value)
			if name != "" {
				required[name] = struct{}{}
			}
		}
	}
	return required
}
