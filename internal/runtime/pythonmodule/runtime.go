package pythonmodule

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bytecodealliance/wasmtime-go/v46"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
)

const (
	Kind         = "python"
	ABI          = "python-json-v1"
	DefaultEntry = "handle"

	Interpreter         = "cpython-wasi-3.13.14-wasi_sdk-24"
	InterpreterDigest   = "sha256:a70a750951302744d84746340b4932b80dabad5b153cc3392bcae114b293c060"
	HarnessABI          = "swarm-python-json-v1"
	artifactZipPath     = "testdata/python-3.13.14-wasi_sdk-24.zip"
	pythonWasmPath      = "python.wasm"
	artifactCachePerm   = 0o755
	artifactFilePerm    = 0o644
	wasmPageSize        = 64 * 1024
	defaultValidateFuel = 2_000_000_000
	wasiErrnoSuccess    = int32(0)
	wasiErrnoFault      = int32(21)
	wasiErrnoNotSup     = int32(58)
)

//go:embed testdata/python-3.13.14-wasi_sdk-24.zip
var artifactFS embed.FS

var (
	artifactOnce sync.Once
	artifactDir  string
	artifactErr  error
)

type Identity struct {
	Interpreter       string
	InterpreterDigest string
	SnapshotDigest    string
	HarnessABI        string
	Engine            string
}

type Request struct {
	ModuleID    string
	RowID       string
	Digest      string
	Entry       string
	Source      []byte
	Input       []byte
	Fuel        uint64
	MemoryPages uint32
	OutputBytes int
}

type Result struct {
	Output         []byte
	FuelConsumed   uint64
	Engine         string
	OutputHash     string
	Interpreter    string
	InterpreterSHA string
	SnapshotHash   string
	HarnessABI     string
	SourceHash     string
}

func RuntimeIdentity() Identity {
	return Identity{
		Interpreter:       Interpreter,
		InterpreterDigest: InterpreterDigest,
		SnapshotDigest:    embeddedSnapshotDigest(),
		HarnessABI:        HarnessABI,
		Engine:            computemodule.EngineVersion(),
	}
}

func ValidateSource(ctx context.Context, req Request) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	req.Entry = normalizeEntry(req.Entry)
	if err := validateRequestShape(req); err != nil {
		return err
	}
	if err := computemodule.ValidateDigest(req.Source, req.Digest); err != nil {
		return &computemodule.Error{Code: computemodule.CodeDigest, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	fuel := req.Fuel
	if fuel == 0 {
		fuel = defaultValidateFuel
	}
	_, err := runHarness(ctx, req, harnessEnvelope{
		Mode:   "validate",
		Source: string(req.Source),
		Entry:  req.Entry,
	}, fuel)
	return err
}

func Execute(ctx context.Context, req Request) (Result, error) {
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	req.Entry = normalizeEntry(req.Entry)
	if err := validateRequestShape(req); err != nil {
		return Result{}, err
	}
	if err := computemodule.ValidateDigest(req.Source, req.Digest); err != nil {
		return Result{}, &computemodule.Error{Code: computemodule.CodeDigest, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	if req.Fuel == 0 {
		return Result{}, &computemodule.Error{Code: computemodule.CodeFuel, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("fuel limit is required")}
	}
	var input map[string]any
	if err := json.Unmarshal(req.Input, &input); err != nil {
		return Result{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("input is not JSON object: %w", err)}
	}
	if input == nil {
		return Result{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("input is not JSON object")}
	}
	wire, err := runHarness(ctx, req, harnessEnvelope{
		Mode:   "execute",
		Source: string(req.Source),
		Entry:  req.Entry,
		Input:  input,
	}, req.Fuel)
	if err != nil {
		return Result{}, err
	}
	output, err := computemodule.CanonicalJSONBytes(wire.Output)
	if err != nil {
		return Result{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("output cannot be encoded as JSON object: %w", err)}
	}
	if req.OutputBytes > 0 && len(output) > req.OutputBytes {
		return Result{}, &computemodule.Error{Code: computemodule.CodeOutputSize, ModuleID: req.ModuleID, RowID: req.RowID, Limit: req.OutputBytes, Actual: len(output), Err: fmt.Errorf("output %d bytes exceeds cap %d", len(output), req.OutputBytes)}
	}
	identity := RuntimeIdentity()
	return Result{
		Output:         output,
		FuelConsumed:   wire.FuelConsumed,
		Engine:         identity.Engine,
		OutputHash:     computemodule.HashBytes(output),
		Interpreter:    identity.Interpreter,
		InterpreterSHA: identity.InterpreterDigest,
		SnapshotHash:   identity.SnapshotDigest,
		HarnessABI:     identity.HarnessABI,
		SourceHash:     sourceHash(req.Source),
	}, nil
}

type harnessEnvelope struct {
	Mode   string         `json:"mode"`
	Source string         `json:"source"`
	Entry  string         `json:"entry"`
	Input  map[string]any `json:"input,omitempty"`
}

type harnessWireResult struct {
	OK           bool           `json:"ok"`
	Code         string         `json:"code,omitempty"`
	Message      string         `json:"message,omitempty"`
	Line         int            `json:"line,omitempty"`
	Output       map[string]any `json:"output,omitempty"`
	FuelConsumed uint64         `json:"-"`
}

func runHarness(ctx context.Context, req Request, envelope harnessEnvelope, fuel uint64) (harnessWireResult, error) {
	if err := contextError(ctx); err != nil {
		return harnessWireResult{}, err
	}
	root, err := materializedArtifactDir()
	if err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	wasm, err := os.ReadFile(filepath.Join(root, pythonWasmPath))
	if err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("read embedded %s: %w", pythonWasmPath, err)}
	}
	input, err := json.Marshal(envelope)
	if err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	runDir, err := os.MkdirTemp("", "swarm-python-module-")
	if err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	defer os.RemoveAll(runDir)
	stdinPath := filepath.Join(runDir, "stdin.json")
	stdoutPath := filepath.Join(runDir, "stdout.json")
	stderrPath := filepath.Join(runDir, "stderr.txt")
	if err := os.WriteFile(stdinPath, input, 0o600); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}

	cfg := wasmtime.NewConfig()
	cfg.SetConsumeFuel(true)
	cfg.SetEpochInterruption(true)
	cfg.SetWasmBulkMemory(true)
	cfg.SetWasmMemory64(false)
	cfg.SetWasmMultiMemory(false)
	cfg.SetWasmSIMD(false)
	cfg.SetWasmRelaxedSIMD(false)
	cfg.SetWasmThreads(false)
	engine := wasmtime.NewEngineWithConfig(cfg)
	module, err := wasmtime.NewModule(engine, wasm)
	if err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	store := wasmtime.NewStore(engine)
	store.SetEpochDeadline(1)
	store.Limiter(memoryLimitBytes(req.MemoryPages), -1, -1, -1, -1)
	if err := store.SetFuel(fuel); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeFuel, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	wasi := wasmtime.NewWasiConfig()
	wasi.SetArgv([]string{"python", "-c", harnessSource})
	wasi.SetEnv(
		[]string{"PYTHONHOME", "PYTHONPATH", "PYTHONHASHSEED", "PYTHONNOUSERSITE"},
		[]string{"/", "/lib/python3.13", "0", "1"},
	)
	if err := wasi.SetStdinFile(stdinPath); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	if err := wasi.SetStdoutFile(stdoutPath); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	if err := wasi.SetStderrFile(stderrPath); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	if err := wasi.PreopenDir(root, "/", wasmtime.DIR_READ, wasmtime.FILE_READ); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	store.SetWasi(wasi)
	linker := wasmtime.NewLinker(engine)
	linker.AllowShadowing(true)
	if err := linker.DefineWasi(); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	if err := defineDeterministicWASI(linker); err != nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
	}
	instance, err := linker.Instantiate(store, module)
	if err != nil {
		return harnessWireResult{}, classifyPythonCallError(req, computemodule.CodeTrap, err)
	}
	start := instance.GetFunc(store, "_start")
	if start == nil {
		return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("embedded interpreter missing _start")}
	}
	callDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			engine.IncrementEpoch()
		case <-callDone:
		}
	}()
	_, callErr := start.Call(store)
	close(callDone)
	if err := ctx.Err(); err != nil {
		return harnessWireResult{}, err
	}
	fuelConsumed := fuel
	if remaining, err := store.GetFuel(); err == nil && remaining <= fuel {
		fuelConsumed = fuel - remaining
	}
	stdout, _ := os.ReadFile(stdoutPath)
	stderr, _ := os.ReadFile(stderrPath)
	var wire harnessWireResult
	if len(bytes.TrimSpace(stdout)) > 0 {
		if err := json.Unmarshal(bytes.TrimSpace(stdout), &wire); err == nil {
			wire.FuelConsumed = fuelConsumed
			if wire.OK {
				if wire.Output == nil {
					return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("python harness returned ok without output")}
				}
				return wire, nil
			}
			return harnessWireResult{}, harnessError(req, wire)
		}
	}
	if callErr != nil {
		return harnessWireResult{}, classifyPythonCallError(req, computemodule.CodeTrap, fmt.Errorf("%w; stderr=%s", callErr, strings.TrimSpace(string(stderr))))
	}
	return harnessWireResult{}, &computemodule.Error{Code: computemodule.CodeABI, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("python harness emitted invalid response stdout=%q stderr=%q", strings.TrimSpace(string(stdout)), strings.TrimSpace(string(stderr)))}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("python execution context is required")
	}
	return ctx.Err()
}

func defineDeterministicWASI(linker *wasmtime.Linker) error {
	if err := linker.FuncWrap("wasi_snapshot_preview1", "random_get", deterministicRandomGet); err != nil {
		return err
	}
	if err := linker.FuncWrap("wasi_snapshot_preview1", "clock_time_get", deterministicClockTimeGet); err != nil {
		return err
	}
	return linker.FuncWrap("wasi_snapshot_preview1", "poll_oneoff", deterministicPollOneoff)
}

func deterministicRandomGet(caller *wasmtime.Caller, ptr int32, length int32) int32 {
	data, ok := callerMemoryData(caller)
	if !ok || ptr < 0 || length < 0 || int64(ptr)+int64(length) > int64(len(data)) {
		return wasiErrnoFault
	}
	for idx := int32(0); idx < length; idx++ {
		data[int(ptr+idx)] = 0
	}
	return wasiErrnoSuccess
}

func deterministicClockTimeGet(caller *wasmtime.Caller, _ int32, _ int64, resultPtr int32) int32 {
	data, ok := callerMemoryData(caller)
	if !ok || resultPtr < 0 || int64(resultPtr)+8 > int64(len(data)) {
		return wasiErrnoFault
	}
	binary.LittleEndian.PutUint64(data[int(resultPtr):int(resultPtr)+8], 0)
	return wasiErrnoSuccess
}

func deterministicPollOneoff(_ *wasmtime.Caller, _ int32, _ int32, _ int32, _ int32) int32 {
	return wasiErrnoNotSup
}

func callerMemoryData(caller *wasmtime.Caller) ([]byte, bool) {
	if caller == nil {
		return nil, false
	}
	ext := caller.GetExport("memory")
	if ext == nil || ext.Memory() == nil {
		return nil, false
	}
	return ext.Memory().UnsafeData(caller), true
}

func harnessError(req Request, wire harnessWireResult) error {
	code := computemodule.CodeABI
	switch wire.Code {
	case string(computemodule.CodeDeniedCapability):
		code = computemodule.CodeDeniedCapability
	case string(computemodule.CodeCompile):
		code = computemodule.CodeCompile
	case string(computemodule.CodePythonException):
		code = computemodule.CodePythonException
	case string(computemodule.CodeABI):
		code = computemodule.CodeABI
	}
	msg := strings.TrimSpace(wire.Message)
	if wire.Line > 0 {
		msg = fmt.Sprintf("%s at line %d", msg, wire.Line)
	}
	return &computemodule.Error{Code: code, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("%s", msg)}
}

func classifyPythonCallError(req Request, fallback computemodule.Code, err error) error {
	code := computemodule.WasmtimeFailureCode(err, fallback)
	return &computemodule.Error{Code: code, ModuleID: req.ModuleID, RowID: req.RowID, Err: err}
}

func validateRequestShape(req Request) error {
	if strings.TrimSpace(req.ModuleID) == "" {
		return &computemodule.Error{Code: computemodule.CodeABI, RowID: req.RowID, Err: fmt.Errorf("module id is required")}
	}
	if len(req.Source) == 0 {
		return &computemodule.Error{Code: computemodule.CodeCompile, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("python source is required")}
	}
	if req.MemoryPages == 0 {
		return &computemodule.Error{Code: computemodule.CodeMemory, ModuleID: req.ModuleID, RowID: req.RowID, Err: fmt.Errorf("memory page limit is required")}
	}
	if req.OutputBytes <= 0 {
		return &computemodule.Error{Code: computemodule.CodeOutputSize, ModuleID: req.ModuleID, RowID: req.RowID, Limit: req.OutputBytes, Actual: 0, Err: fmt.Errorf("output byte limit is required")}
	}
	return nil
}

func materializedArtifactDir() (string, error) {
	artifactOnce.Do(func() {
		artifactDir, artifactErr = extractArtifact()
	})
	return artifactDir, artifactErr
}

func extractArtifact() (string, error) {
	raw, err := artifactFS.ReadFile(artifactZipPath)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	if actual != InterpreterDigest {
		return "", fmt.Errorf("embedded CPython-WASI artifact digest %s does not match declared %s", actual, InterpreterDigest)
	}
	dir, err := os.MkdirTemp("", "swarm-"+Interpreter+"-")
	if err != nil {
		return "", err
	}
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(dir)
		}
	}()
	reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return "", err
	}
	for _, file := range reader.File {
		name := filepath.Clean(filepath.FromSlash(file.Name))
		if name == "." || name == "" || strings.HasPrefix(name, ".."+string(filepath.Separator)) || filepath.IsAbs(name) {
			return "", fmt.Errorf("embedded CPython-WASI artifact contains unsafe path %q", file.Name)
		}
		target := filepath.Join(dir, name)
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(target, artifactCachePerm); err != nil {
				return "", err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), artifactCachePerm); err != nil {
			return "", err
		}
		src, err := file.Open()
		if err != nil {
			return "", err
		}
		dst, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, artifactFilePerm)
		if err != nil {
			src.Close()
			return "", err
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := dst.Close()
		src.Close()
		if copyErr != nil {
			return "", copyErr
		}
		if closeErr != nil {
			return "", closeErr
		}
	}
	success = true
	return dir, nil
}

func embeddedSnapshotDigest() string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		Interpreter,
		InterpreterDigest,
		HarnessABI,
		harnessSource,
		"preimports=json,ast,builtins",
	}, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func sourceHash(source []byte) string {
	sum := sha256.Sum256(source)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func normalizeEntry(entry string) string {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return DefaultEntry
	}
	return entry
}

func memoryLimitBytes(memoryPages uint32) int64 {
	return int64(memoryPages) * wasmPageSize
}

const harnessSource = `
import ast
import builtins
import json
import sys

DENIED_CODE = "compute_module_denied_capability"
COMPILE_CODE = "compute_module_compile"
ABI_CODE = "compute_module_abi"
EXCEPTION_CODE = "compute_module_python_exception"
ALLOWED_IMPORTS = {"json", "re", "collections", "itertools", "functools", "operator", "string", "textwrap", "decimal"}
DENIED_NAMES = {"open", "eval", "exec", "compile", "__import__", "__builtins__", "globals", "locals", "vars", "dir", "getattr", "setattr", "delattr", "help", "breakpoint"}
SAFE_BUILTIN_NAMES = {
    "abs", "all", "any", "bool", "dict", "enumerate", "filter", "float", "int", "isinstance",
    "len", "list", "map", "max", "min", "pow", "range", "reversed", "round", "set", "slice",
    "sorted", "str", "sum", "tuple", "zip", "Exception", "ValueError", "TypeError", "KeyError",
}

def emit_error(code, message, line=0):
    sys.stdout.write(json.dumps({"ok": False, "code": code, "message": str(message), "line": int(line or 0)}, sort_keys=True))

def emit_ok(output=None):
    payload = {"ok": True}
    if output is not None:
        payload["output"] = output
    sys.stdout.write(json.dumps(payload, sort_keys=True))

def check_tree(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name.split(".")[0] for alias in node.names]
            elif node.module:
                names = [node.module.split(".")[0]]
            for name in names:
                if name not in ALLOWED_IMPORTS:
                    return DENIED_CODE, "import %s is not available to python compute modules" % name, getattr(node, "lineno", 0)
        if isinstance(node, ast.Name) and node.id in DENIED_NAMES:
            return DENIED_CODE, "%s is not available to python compute modules" % node.id, getattr(node, "lineno", 0)
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            return DENIED_CODE, "%s is not available to python compute modules" % node.id, getattr(node, "lineno", 0)
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            return DENIED_CODE, "%s is not available to python compute modules" % node.attr, getattr(node, "lineno", 0)
    return "", "", 0

def limited_import(name, globals=None, locals=None, fromlist=(), level=0):
    root = name.split(".")[0]
    if root not in ALLOWED_IMPORTS:
        raise PermissionError("import %s is not available to python compute modules" % root)
    return builtins.__import__(name, globals, locals, fromlist, level)

def safe_globals():
    safe = {name: getattr(builtins, name) for name in SAFE_BUILTIN_NAMES if hasattr(builtins, name)}
    safe["__import__"] = limited_import
    return {"__builtins__": safe}

try:
    envelope = json.load(sys.stdin)
    source = envelope.get("source") or ""
    entry = envelope.get("entry") or "handle"
    filename = "<swarm-python-module>"
    tree = ast.parse(source, filename=filename, mode="exec")
    code, message, line = check_tree(tree)
    if code:
        emit_error(code, message, line)
        sys.exit(1)
    compiled = compile(tree, filename, "exec")
    namespace = safe_globals()
    exec(compiled, namespace, namespace)
    handle = namespace.get(entry)
    if not callable(handle):
        emit_error(ABI_CODE, "python module must define callable %s(input)" % entry)
        sys.exit(1)
    if envelope.get("mode") == "validate":
        emit_ok({})
        sys.exit(0)
    result = handle(envelope.get("input") or {})
    if not isinstance(result, dict):
        emit_error(ABI_CODE, "python module %s(input) must return a JSON object/dict" % entry)
        sys.exit(1)
    emit_ok(result)
except SyntaxError as exc:
    emit_error(COMPILE_CODE, exc.msg, exc.lineno or 0)
    sys.exit(1)
except PermissionError as exc:
    emit_error(DENIED_CODE, str(exc))
    sys.exit(1)
except Exception as exc:
    emit_error(EXCEPTION_CODE, "%s: %s" % (exc.__class__.__name__, exc), getattr(exc, "__traceback__", None).tb_lineno if getattr(exc, "__traceback__", None) else 0)
    sys.exit(1)
`
