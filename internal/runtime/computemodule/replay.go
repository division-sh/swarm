package computemodule

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strings"
)

const (
	ReplayEvidenceSchema = "swarm.compute_module.replay.v1"
	ReplayEvidenceAction = "compute_module_replay_evidence"
)

type ReplayOutcome string

const (
	ReplayOutcomeSuccess ReplayOutcome = "success"
	ReplayOutcomeFailure ReplayOutcome = "failure"
)

type ReplayFindingKind string

const (
	ReplayFindingIdentityDivergence ReplayFindingKind = "identity_divergence"
	ReplayFindingUnsupportedProfile ReplayFindingKind = "unsupported_profile"
	ReplayFindingResultDivergence   ReplayFindingKind = "result_divergence"
	ReplayFindingResourceDivergence ReplayFindingKind = "resource_envelope_divergence"
)

type ReplayLimits struct {
	Fuel        uint64 `json:"fuel"`
	MemoryPages uint32 `json:"memory_pages"`
	OutputBytes int    `json:"output_bytes"`
}

type ReplayEnvelope struct {
	ModuleID          string        `json:"module_id"`
	RowID             string        `json:"row_id"`
	Kind              string        `json:"kind"`
	ABI               string        `json:"abi"`
	Entry             string        `json:"entry"`
	Digest            string        `json:"digest"`
	SourceHash        string        `json:"source_hash,omitempty"`
	InputHash         string        `json:"input_hash"`
	Outcome           ReplayOutcome `json:"outcome"`
	OutputHash        string        `json:"output_hash,omitempty"`
	ErrorCode         string        `json:"error_code,omitempty"`
	FuelConsumed      uint64        `json:"fuel_consumed"`
	Limits            ReplayLimits  `json:"limits"`
	Engine            string        `json:"engine"`
	Arch              string        `json:"arch"`
	Interpreter       string        `json:"interpreter,omitempty"`
	InterpreterDigest string        `json:"interpreter_digest,omitempty"`
	SnapshotDigest    string        `json:"snapshot_digest,omitempty"`
	HarnessABI        string        `json:"harness_abi,omitempty"`
}

func (e ReplayEnvelope) Normalized() ReplayEnvelope {
	e.ModuleID = strings.TrimSpace(e.ModuleID)
	e.RowID = strings.TrimSpace(e.RowID)
	e.Kind = strings.TrimSpace(e.Kind)
	if e.Kind == "" {
		e.Kind = "wasm"
	}
	e.ABI = strings.TrimSpace(e.ABI)
	e.Entry = strings.TrimSpace(e.Entry)
	e.Digest = strings.TrimSpace(e.Digest)
	e.SourceHash = strings.TrimSpace(e.SourceHash)
	e.InputHash = strings.TrimSpace(e.InputHash)
	e.Outcome = ReplayOutcome(strings.TrimSpace(string(e.Outcome)))
	e.OutputHash = strings.TrimSpace(e.OutputHash)
	e.ErrorCode = strings.TrimSpace(e.ErrorCode)
	e.Engine = strings.TrimSpace(e.Engine)
	e.Arch = strings.TrimSpace(e.Arch)
	e.Interpreter = strings.TrimSpace(e.Interpreter)
	e.InterpreterDigest = strings.TrimSpace(e.InterpreterDigest)
	e.SnapshotDigest = strings.TrimSpace(e.SnapshotDigest)
	e.HarnessABI = strings.TrimSpace(e.HarnessABI)
	return e
}

func CurrentArch() string {
	return runtime.GOARCH
}

type ReplayFinding struct {
	Schema      string            `json:"schema"`
	Kind        ReplayFindingKind `json:"kind"`
	Field       string            `json:"field"`
	ModuleID    string            `json:"module_id,omitempty"`
	RowID       string            `json:"row_id,omitempty"`
	Expected    string            `json:"expected,omitempty"`
	Actual      string            `json:"actual,omitempty"`
	Message     string            `json:"message"`
	Remediation string            `json:"remediation"`
}

func (f ReplayFinding) Normalized() ReplayFinding {
	f.Schema = strings.TrimSpace(f.Schema)
	if f.Schema == "" {
		f.Schema = ReplayEvidenceSchema
	}
	f.Kind = ReplayFindingKind(strings.TrimSpace(string(f.Kind)))
	f.Field = strings.TrimSpace(f.Field)
	f.ModuleID = strings.TrimSpace(f.ModuleID)
	f.RowID = strings.TrimSpace(f.RowID)
	f.Expected = strings.TrimSpace(f.Expected)
	f.Actual = strings.TrimSpace(f.Actual)
	f.Message = strings.TrimSpace(f.Message)
	f.Remediation = strings.TrimSpace(f.Remediation)
	return f
}

type ReplayEvidenceDetail struct {
	Schema    string           `json:"schema"`
	Envelopes []ReplayEnvelope `json:"envelopes"`
}

func NewReplayEvidenceDetail(envelopes []ReplayEnvelope) map[string]any {
	out := make([]ReplayEnvelope, 0, len(envelopes))
	for _, envelope := range envelopes {
		out = append(out, envelope.Normalized())
	}
	return map[string]any{
		"compute_module_replay": ReplayEvidenceDetail{
			Schema:    ReplayEvidenceSchema,
			Envelopes: out,
		},
	}
}

func DecodeReplayEvidenceDetail(detail map[string]any) ([]ReplayEnvelope, error) {
	if len(detail) == 0 {
		return nil, nil
	}
	raw, ok := detail["compute_module_replay"]
	if !ok || raw == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("encode compute_module replay detail: %w", err)
	}
	var payload ReplayEvidenceDetail
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return nil, fmt.Errorf("decode compute_module replay detail: %w", err)
	}
	if strings.TrimSpace(payload.Schema) != ReplayEvidenceSchema {
		return nil, fmt.Errorf("unsupported compute_module replay detail schema %q", payload.Schema)
	}
	out := make([]ReplayEnvelope, 0, len(payload.Envelopes))
	for _, envelope := range payload.Envelopes {
		out = append(out, envelope.Normalized())
	}
	return out, nil
}

func CompareReplayEnvelopes(expected, actual ReplayEnvelope) *ReplayFinding {
	expected = expected.Normalized()
	actual = actual.Normalized()
	identityFields := []struct {
		name string
		want string
		got  string
	}{
		{"module_id", expected.ModuleID, actual.ModuleID},
		{"row_id", expected.RowID, actual.RowID},
		{"kind", expected.Kind, actual.Kind},
		{"abi", expected.ABI, actual.ABI},
		{"entry", expected.Entry, actual.Entry},
		{"digest", expected.Digest, actual.Digest},
		{"source_hash", expected.SourceHash, actual.SourceHash},
		{"input_hash", expected.InputHash, actual.InputHash},
		{"limits.fuel", fmt.Sprint(expected.Limits.Fuel), fmt.Sprint(actual.Limits.Fuel)},
		{"limits.memory_pages", fmt.Sprint(expected.Limits.MemoryPages), fmt.Sprint(actual.Limits.MemoryPages)},
		{"limits.output_bytes", fmt.Sprint(expected.Limits.OutputBytes), fmt.Sprint(actual.Limits.OutputBytes)},
		{"interpreter", expected.Interpreter, actual.Interpreter},
		{"interpreter_digest", expected.InterpreterDigest, actual.InterpreterDigest},
		{"snapshot_digest", expected.SnapshotDigest, actual.SnapshotDigest},
		{"harness_abi", expected.HarnessABI, actual.HarnessABI},
	}
	for _, field := range identityFields {
		if field.want != field.got {
			return replayFinding(ReplayFindingIdentityDivergence, field.name, expected, actual, field.want, field.got)
		}
	}
	profileFields := []struct {
		name string
		want string
		got  string
	}{
		{"engine", expected.Engine, actual.Engine},
		{"arch", expected.Arch, actual.Arch},
	}
	for _, field := range profileFields {
		if field.want != field.got {
			return replayFinding(ReplayFindingUnsupportedProfile, field.name, expected, actual, field.want, field.got)
		}
	}
	if expected.Outcome != actual.Outcome {
		return replayFinding(ReplayFindingResultDivergence, "outcome", expected, actual, string(expected.Outcome), string(actual.Outcome))
	}
	switch actual.Outcome {
	case ReplayOutcomeFailure:
		if expected.ErrorCode != actual.ErrorCode {
			return replayFinding(ReplayFindingResultDivergence, "error_code", expected, actual, expected.ErrorCode, actual.ErrorCode)
		}
	default:
		if expected.OutputHash != actual.OutputHash {
			return replayFinding(ReplayFindingResultDivergence, "output_hash", expected, actual, expected.OutputHash, actual.OutputHash)
		}
	}
	if actual.Kind != "python" && expected.FuelConsumed != actual.FuelConsumed {
		return replayFinding(ReplayFindingResourceDivergence, "fuel_consumed", expected, actual, fmt.Sprint(expected.FuelConsumed), fmt.Sprint(actual.FuelConsumed))
	}
	return nil
}

func replayFinding(kind ReplayFindingKind, field string, expected, actual ReplayEnvelope, want, got string) *ReplayFinding {
	remediation := "re-run with the original compute_module bundle, module bytes, runtime profile, and input evidence"
	if kind == ReplayFindingResultDivergence {
		remediation = "treat this as a deterministic replay correctness failure and inspect the module/runtime determinism boundary"
	}
	if kind == ReplayFindingUnsupportedProfile {
		remediation = "replay this trace with the recorded engine and architecture profile, or regenerate replay evidence under the current profile"
	}
	if kind == ReplayFindingResourceDivergence {
		remediation = "inspect fuel/resource accounting under the recorded engine profile before accepting replay"
	}
	finding := ReplayFinding{
		Schema:      ReplayEvidenceSchema,
		Kind:        kind,
		Field:       field,
		ModuleID:    firstNonEmpty(actual.ModuleID, expected.ModuleID),
		RowID:       firstNonEmpty(actual.RowID, expected.RowID),
		Expected:    want,
		Actual:      got,
		Message:     fmt.Sprintf("compute_module replay %s on %s", kind, field),
		Remediation: remediation,
	}.Normalized()
	return &finding
}

func CanonicalJSONBytes(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonicalizeJSON(raw)
}

func CanonicalizeJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("trailing JSON content")
		}
		return nil, fmt.Errorf("trailing JSON content")
	}
	out, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func CanonicalJSONHash(v any) (string, error) {
	raw, err := CanonicalJSONBytes(v)
	if err != nil {
		return "", err
	}
	return HashBytes(raw), nil
}

func CanonicalJSONHashRaw(raw []byte) (string, error) {
	canonical, err := CanonicalizeJSON(raw)
	if err != nil {
		return "", err
	}
	return HashBytes(canonical), nil
}

func HashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
