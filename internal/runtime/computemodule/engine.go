package computemodule

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/bytecodealliance/wasmtime-go/v46"
)

const (
	ABI           = "core-json-v1"
	DefaultEntry  = "compute"
	memoryExport  = "memory"
	allocExport   = "alloc"
	versionModule = "github.com/bytecodealliance/wasmtime-go/v46"
	deterministic = "compute_module_deterministic_no_retry"
	wasmPageSize  = 64 * 1024
)

type Code string

const (
	CodeCompile          Code = "compute_module_compile"
	CodeDigest           Code = "compute_module_digest"
	CodeImport           Code = "compute_module_import"
	CodeABI              Code = "compute_module_abi"
	CodeMemory           Code = "compute_module_memory_limit"
	CodeFuel             Code = "compute_module_fuel_exhausted"
	CodeTrap             Code = "compute_module_trap"
	CodeOutputSize       Code = "compute_module_output_size"
	CodeOutputBounds     Code = "compute_module_output_bounds"
	CodeDeniedCapability Code = "compute_module_denied_capability"
	CodePythonException  Code = "compute_module_python_exception"
	CodeReplay           Code = "compute_module_replay_divergence"
)

type ModuleImport struct {
	Module string
	Name   string
	Kind   string
}

type Inspection struct {
	Imports []ModuleImport
	Exports map[string]string
}

type Request struct {
	ModuleID    string
	RowID       string
	Digest      string
	Entry       string
	Wasm        []byte
	Input       []byte
	Fuel        uint64
	MemoryPages uint32
	OutputBytes int
}

type Result struct {
	Output       []byte
	FuelConsumed uint64
	Engine       string
	OutputHash   string
}

type Error struct {
	Code     Code
	ModuleID string
	RowID    string
	Err      error
}

func (e *Error) Error() string {
	parts := []string{deterministic, string(e.Code)}
	if strings.TrimSpace(e.ModuleID) != "" {
		parts = append(parts, "module="+strings.TrimSpace(e.ModuleID))
	}
	if strings.TrimSpace(e.RowID) != "" {
		parts = append(parts, "row="+strings.TrimSpace(e.RowID))
	}
	if e.Err != nil {
		parts = append(parts, e.Err.Error())
	}
	return strings.Join(parts, ": ")
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsDeterministicFailure(err error) bool {
	if err == nil {
		return false
	}
	var typed *Error
	return errors.As(err, &typed)
}

func Inspect(wasm []byte) (Inspection, error) {
	engine := newEngine()
	module, err := wasmtime.NewModule(engine, wasm)
	if err != nil {
		return Inspection{}, &Error{Code: CodeCompile, Err: err}
	}
	inspection := Inspection{Exports: map[string]string{}}
	for _, imp := range module.Imports() {
		name := ""
		if imp.Name() != nil {
			name = *imp.Name()
		}
		inspection.Imports = append(inspection.Imports, ModuleImport{
			Module: strings.TrimSpace(imp.Module()),
			Name:   strings.TrimSpace(name),
			Kind:   externKind(imp.Type()),
		})
	}
	for _, exp := range module.Exports() {
		inspection.Exports[strings.TrimSpace(exp.Name())] = externKind(exp.Type())
	}
	return inspection, nil
}

func ValidateCoreJSONModule(wasm []byte, entry string, memoryPages uint32) error {
	entry = normalizeEntry(entry)
	engine := newEngine()
	module, err := wasmtime.NewModule(engine, wasm)
	if err != nil {
		return &Error{Code: CodeCompile, Err: err}
	}
	if imports := module.Imports(); len(imports) > 0 {
		imp := imports[0]
		name := ""
		if imp.Name() != nil {
			name = *imp.Name()
		}
		return &Error{Code: CodeImport, Err: fmt.Errorf("module declares unsupported import %s.%s", strings.TrimSpace(imp.Module()), strings.TrimSpace(name))}
	}
	exports := map[string]*wasmtime.ExternType{}
	for _, exp := range module.Exports() {
		exports[strings.TrimSpace(exp.Name())] = exp.Type()
	}
	if err := requireMemoryExport(exports, memoryPages); err != nil {
		return err
	}
	if err := requireFuncExport(exports, allocExport, []wasmtime.ValKind{wasmtime.KindI32}, []wasmtime.ValKind{wasmtime.KindI32}); err != nil {
		return err
	}
	if err := requireFuncExport(exports, entry, []wasmtime.ValKind{wasmtime.KindI32, wasmtime.KindI32}, []wasmtime.ValKind{wasmtime.KindI64}); err != nil {
		return err
	}
	return nil
}

func Execute(req Request) (Result, error) {
	req.Entry = normalizeEntry(req.Entry)
	if err := ValidateDigest(req.Wasm, req.Digest); err != nil {
		return Result{}, &Error{Code: CodeDigest, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	if err := ValidateCoreJSONModule(req.Wasm, req.Entry, req.MemoryPages); err != nil {
		return Result{}, withSite(err, req.ModuleID, req.RowID)
	}
	engine := newEngine()
	module, err := wasmtime.NewModule(engine, req.Wasm)
	if err != nil {
		return Result{}, &Error{Code: CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	store := wasmtime.NewStore(engine)
	store.Limiter(memoryLimitBytes(req.MemoryPages), -1, -1, -1, -1)
	if err := store.SetFuel(req.Fuel); err != nil {
		return Result{}, &Error{Code: CodeFuel, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	instance, err := wasmtime.NewInstance(store, module, nil)
	if err != nil {
		return Result{}, classifyCallError(req, CodeTrap, err)
	}
	memoryExt := instance.GetExport(store, memoryExport)
	if memoryExt == nil || memoryExt.Memory() == nil {
		return Result{}, &Error{Code: CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("missing %s memory export", memoryExport)}
	}
	memory := memoryExt.Memory()
	if memory.Size(store) > uint64(req.MemoryPages) {
		return Result{}, &Error{Code: CodeMemory, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("memory size %d exceeds limit %d pages", memory.Size(store), req.MemoryPages)}
	}
	alloc := instance.GetFunc(store, allocExport)
	if alloc == nil {
		return Result{}, &Error{Code: CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("missing %s function export", allocExport)}
	}
	compute := instance.GetFunc(store, req.Entry)
	if compute == nil {
		return Result{}, &Error{Code: CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("missing %s function export", req.Entry)}
	}
	allocResult, err := alloc.Call(store, int32(len(req.Input)))
	if err != nil {
		return Result{}, classifyCallError(req, CodeTrap, err)
	}
	inputPtr, ok := toInt32(allocResult)
	if !ok || inputPtr < 0 {
		return Result{}, &Error{Code: CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("alloc returned invalid pointer %v", allocResult)}
	}
	if err := writeMemory(memory, store, uint32(inputPtr), req.Input); err != nil {
		return Result{}, &Error{Code: CodeOutputBounds, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	raw, err := compute.Call(store, inputPtr, int32(len(req.Input)))
	if err != nil {
		return Result{}, classifyCallError(req, CodeTrap, err)
	}
	packed, ok := toInt64(raw)
	if !ok {
		return Result{}, &Error{Code: CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("%s returned non-i64 value %T", req.Entry, raw)}
	}
	outputPtr := uint32(uint64(packed) >> 32)
	outputLen := uint32(uint64(packed) & 0xffffffff)
	if req.OutputBytes > 0 && uint64(outputLen) > uint64(req.OutputBytes) {
		return Result{}, &Error{Code: CodeOutputSize, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("output %d bytes exceeds cap %d", outputLen, req.OutputBytes)}
	}
	output, err := readMemory(memory, store, outputPtr, outputLen)
	if err != nil {
		return Result{}, &Error{Code: CodeOutputBounds, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	fuelConsumed := req.Fuel
	if remaining, err := store.GetFuel(); err == nil && remaining <= req.Fuel {
		fuelConsumed = req.Fuel - remaining
	}
	sum := sha256.Sum256(output)
	return Result{
		Output:       output,
		FuelConsumed: fuelConsumed,
		Engine:       EngineVersion(),
		OutputHash:   "sha256:" + hex.EncodeToString(sum[:]),
	}, nil
}

func EngineVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == versionModule {
				return versionModule + ":" + dep.Version
			}
		}
	}
	return versionModule + ":unknown"
}

func newEngine() *wasmtime.Engine {
	cfg := wasmtime.NewConfig()
	cfg.SetConsumeFuel(true)
	cfg.SetWasmBulkMemory(true)
	cfg.SetWasmMemory64(false)
	cfg.SetWasmMultiMemory(false)
	cfg.SetWasmSIMD(false)
	cfg.SetWasmRelaxedSIMD(false)
	cfg.SetWasmThreads(false)
	return wasmtime.NewEngineWithConfig(cfg)
}

func normalizeEntry(entry string) string {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return DefaultEntry
	}
	return entry
}

func externKind(ty *wasmtime.ExternType) string {
	switch {
	case ty == nil:
		return ""
	case ty.FuncType() != nil:
		return "func"
	case ty.MemoryType() != nil:
		return "memory"
	case ty.TableType() != nil:
		return "table"
	case ty.GlobalType() != nil:
		return "global"
	default:
		return "unknown"
	}
}

func ValidateDigest(wasm []byte, digest string) error {
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return fmt.Errorf("module digest is required")
	}
	sum := sha256.Sum256(wasm)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	if digest != actual {
		return fmt.Errorf("module digest %s does not match module bytes %s", digest, actual)
	}
	return nil
}

func requireMemoryExport(exports map[string]*wasmtime.ExternType, memoryPages uint32) error {
	ty := exports[memoryExport]
	if ty == nil || ty.MemoryType() == nil {
		return &Error{Code: CodeABI, Err: fmt.Errorf("missing %s memory export", memoryExport)}
	}
	mem := ty.MemoryType()
	if mem.Is64() {
		return &Error{Code: CodeMemory, Err: fmt.Errorf("%s memory64 is not supported", memoryExport)}
	}
	maxSet, max := mem.Maximum()
	if !maxSet {
		return &Error{Code: CodeMemory, Err: fmt.Errorf("%s memory must declare a maximum", memoryExport)}
	}
	if memoryPages == 0 {
		return &Error{Code: CodeMemory, Err: fmt.Errorf("memory page limit is required")}
	}
	if max > uint64(memoryPages) {
		return &Error{Code: CodeMemory, Err: fmt.Errorf("%s maximum %d pages exceeds declared limit %d", memoryExport, max, memoryPages)}
	}
	return nil
}

func memoryLimitBytes(memoryPages uint32) int64 {
	return int64(memoryPages) * wasmPageSize
}

func requireFuncExport(exports map[string]*wasmtime.ExternType, name string, params, results []wasmtime.ValKind) error {
	ty := exports[name]
	if ty == nil || ty.FuncType() == nil {
		return &Error{Code: CodeABI, Err: fmt.Errorf("missing %s function export", name)}
	}
	funcTy := ty.FuncType()
	if !sameValKinds(funcTy.Params(), params) || !sameValKinds(funcTy.Results(), results) {
		return &Error{Code: CodeABI, Err: fmt.Errorf("%s signature must be %s -> %s", name, valKindSummary(params), valKindSummary(results))}
	}
	return nil
}

func sameValKinds(types []*wasmtime.ValType, want []wasmtime.ValKind) bool {
	if len(types) != len(want) {
		return false
	}
	for idx, ty := range types {
		if ty == nil || ty.Kind() != want[idx] {
			return false
		}
	}
	return true
}

func valKindSummary(kinds []wasmtime.ValKind) string {
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, kind.String())
	}
	return "(" + strings.Join(parts, ",") + ")"
}

func withSite(err error, moduleID, rowID string) error {
	var typed *Error
	if errors.As(err, &typed) {
		return &Error{Code: typed.Code, ModuleID: moduleID, RowID: rowID, Err: typed.Err}
	}
	return err
}

func classifyCallError(req Request, fallback Code, err error) error {
	code := fallback
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "fuel"):
		code = CodeFuel
	case strings.Contains(message, "memory") || strings.Contains(message, "resource limiter"):
		code = CodeMemory
	}
	return &Error{Code: code, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
}

func writeMemory(memory *wasmtime.Memory, store *wasmtime.Store, ptr uint32, input []byte) error {
	data := memory.UnsafeData(store)
	end := uint64(ptr) + uint64(len(input))
	if end > uint64(len(data)) {
		return fmt.Errorf("write [%d:%d] exceeds memory size %d", ptr, end, len(data))
	}
	copy(data[ptr:end], input)
	return nil
}

func readMemory(memory *wasmtime.Memory, store *wasmtime.Store, ptr, length uint32) ([]byte, error) {
	data := memory.UnsafeData(store)
	end := uint64(ptr) + uint64(length)
	if end > uint64(len(data)) {
		return nil, fmt.Errorf("read [%d:%d] exceeds memory size %d", ptr, end, len(data))
	}
	out := make([]byte, length)
	copy(out, data[ptr:end])
	return out, nil
}

func toInt32(value any) (int32, bool) {
	switch typed := value.(type) {
	case int32:
		return typed, true
	case int:
		return int32(typed), int64(typed) == int64(int32(typed))
	default:
		return 0, false
	}
}

func toInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	default:
		return 0, false
	}
}
