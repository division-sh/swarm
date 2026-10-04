package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

const (
	Argument = "--internal-workspace-worker"
	ABI      = "swarm-workspace-worker-v2"
	MaxBytes = 4 << 20
)

type Identity struct {
	ABI          string                `json:"abi"`
	OS           string                `json:"os"`
	Arch         string                `json:"arch"`
	BinaryDigest string                `json:"binary_digest"`
	Interpreter  pythonmodule.Identity `json:"interpreter"`
	Version      string                `json:"version"`
	Revision     string                `json:"revision"`
	Modified     bool                  `json:"modified"`
}

var buildVersion, buildRevision = "dev", "unknown"

func ConfigureBuildMetadata(version, revision string) {
	buildVersion, buildRevision = version, revision
}

type Request struct {
	Mode       string                      `json:"mode"`
	Expected   Identity                    `json:"expected"`
	Gateway    toolgateway.HTTPObservation `json:"gateway"`
	Module     *pythonmodule.Request       `json:"module,omitempty"`
	Tool       string                      `json:"tool,omitempty"`
	Arguments  json.RawMessage             `json:"arguments,omitempty"`
	Occurrence string                      `json:"occurrence,omitempty"`
	Deadline   time.Time                   `json:"deadline,omitempty"`
}

type Result struct {
	Identity     Identity                       `json:"identity"`
	Definitions  []toolgateway.ListedDefinition `json:"definitions,omitempty"`
	Module       *pythonmodule.Result           `json:"module,omitempty"`
	ToolResult   json.RawMessage                `json:"tool_result,omitempty"`
	Failure      *failures.Envelope             `json:"failure,omitempty"`
	Cancellation string                         `json:"cancellation,omitempty"`
	ModelStarted bool                           `json:"model_started,omitempty"`
}

func ExecutableIdentity() (Identity, error) {
	path, err := os.Executable()
	if err != nil {
		return Identity{}, err
	}
	digest, err := ArtifactDigest(path)
	if err != nil {
		return Identity{}, err
	}
	identity := Identity{ABI: ABI, OS: runtime.GOOS, Arch: runtime.GOARCH, BinaryDigest: digest, Interpreter: pythonmodule.RuntimeIdentity(), Version: buildVersion, Revision: buildRevision}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if identity.Revision == "unknown" || identity.Revision == "" {
					identity.Revision = setting.Value
				}
			case "vcs.modified":
				identity.Modified = setting.Value == "true"
			}
		}
	}
	return identity, nil
}

func ValidateLinuxArtifact(current, artifact Identity) error {
	digest, err := hex.DecodeString(strings.TrimPrefix(artifact.BinaryDigest, "sha256:"))
	if artifact.ABI != current.ABI || artifact.OS != "linux" || (artifact.Arch != "amd64" && artifact.Arch != "arm64") || err != nil || len(digest) != sha256.Size || !strings.HasPrefix(artifact.BinaryDigest, "sha256:") || artifact.Interpreter != current.Interpreter || artifact.Version == "" || artifact.Version != current.Version || artifact.Revision != current.Revision {
		return fmt.Errorf("workspace Linux worker identity does not match the current executable")
	}
	if current.OS != "linux" && (current.Modified || artifact.Modified || current.Revision == "unknown" || current.Revision == "") {
		return fmt.Errorf("cross-platform workspace worker requires a clean, revision-pinned Linux artifact")
	}
	return nil
}

func ArtifactDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func Run(ctx context.Context, input io.Reader, output io.Writer) int {
	raw, err := io.ReadAll(io.LimitReader(input, MaxBytes+1))
	if err != nil || len(raw) > MaxBytes {
		return 2
	}
	var request Request
	if err := decodeMessage(raw, &request); err != nil {
		return 2
	}
	if !request.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, request.Deadline)
		defer cancel()
	}
	identity, err := ExecutableIdentity()
	if err != nil {
		return 2
	}
	result := runRequest(ctx, request, identity)
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return 2
	}
	return 0
}

func runRequest(ctx context.Context, request Request, identity Identity) Result {
	result := Result{Identity: identity}
	var err error
	if request.Mode != "identity" && request.Expected != identity {
		err = failures.New(failures.ClassDependencyUnavailable, "workspace_worker_incompatible", "workspace-worker", "admit", map[string]any{"abi": identity.ABI, "os": identity.OS, "arch": identity.Arch})
	} else {
		err = execute(ctx, request, &result)
	}
	if err != nil {
		if envelope, ok := failures.EnvelopeFromError(err); ok {
			// Cancellation does not revoke evidence that a dispatched call may
			// have committed. Keep the typed outcome instead of bare cancellation.
			result.Failure = &envelope
			return result
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			result.Cancellation = "deadline_exceeded"
		case errors.Is(err, context.Canceled):
			result.Cancellation = "canceled"
		default:
			envelope := failures.Normalize(err, "workspace-worker", request.Mode)
			result.Failure = &envelope
		}
	}
	return result
}

// The child protocol is a closed envelope. Validate duplicate keys without
// rewriting captured module/input bytes, then reject undeclared fields.
func decodeMessage(raw []byte, destination any) error {
	if _, err := canonicaljson.Decode(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func DecodeResult(raw []byte) (Result, error) {
	var result Result
	if err := decodeMessage(raw, &result); err != nil {
		return Result{}, err
	}
	if result.Identity.ABI == "" {
		return Result{}, fmt.Errorf("workspace worker response identity missing")
	}
	if (result.Cancellation != "" && result.Cancellation != "canceled" && result.Cancellation != "deadline_exceeded") || (result.Cancellation != "" && result.Failure != nil) {
		return Result{}, fmt.Errorf("workspace worker response cancellation is invalid")
	}
	return result, nil
}

func (r Result) Err() error {
	switch r.Cancellation {
	case "canceled":
		return context.Canceled
	case "deadline_exceeded":
		return context.DeadlineExceeded
	}
	if r.Failure != nil {
		return failures.FromEnvelope(*r.Failure)
	}
	return nil
}

func execute(ctx context.Context, request Request, result *Result) error {
	switch request.Mode {
	case "identity":
		return nil
	case "gateway":
		return request.Gateway.Initialize(ctx)
	case "probe":
		definitions, err := request.Gateway.Probe(ctx)
		result.Definitions = definitions
		return err
	case "model":
		module := request.Module
		if module == nil || module.Entry != mockperformance.EntryHandle || module.Fuel != mockperformance.ExecutionFuel || module.MemoryPages != mockperformance.ExecutionMemoryPages || module.OutputBytes != mockperformance.ExecutionOutputBytes {
			return failures.New(failures.ClassSchemaInvalid, "mock_worker_bounds_invalid", "workspace-worker", "model", nil)
		}
		result.ModelStarted = true
		value, err := pythonmodule.Execute(ctx, *module)
		if err != nil {
			return fmt.Errorf("bounded mock performance: %w", err)
		}
		result.Module = &value
		return nil
	case "call":
		value, err := request.Gateway.Call(ctx, request.Tool, request.Arguments, request.Occurrence)
		result.ToolResult = value
		return err
	default:
		return failures.New(failures.ClassSchemaInvalid, "workspace_worker_mode_invalid", "workspace-worker", "admit", nil)
	}
}
