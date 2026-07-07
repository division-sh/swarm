package computemodule

import "testing"

func TestCanonicalJSONHashIgnoresObjectKeyOrder(t *testing.T) {
	first, err := CanonicalJSONHashRaw([]byte(`{"b":2,"a":{"z":true,"m":["x","y"]}}`))
	if err != nil {
		t.Fatalf("CanonicalJSONHashRaw first: %v", err)
	}
	second, err := CanonicalJSONHashRaw([]byte(`{"a":{"m":["x","y"],"z":true},"b":2}`))
	if err != nil {
		t.Fatalf("CanonicalJSONHashRaw second: %v", err)
	}
	if first != second {
		t.Fatalf("canonical hashes differ: %s != %s", first, second)
	}
	if _, err := CanonicalJSONHashRaw([]byte(`{"a":1}{"b":2}`)); err == nil {
		t.Fatal("CanonicalJSONHashRaw accepted trailing JSON")
	}
}

func TestCompareReplayEnvelopesSeparatesDivergenceClasses(t *testing.T) {
	base := ReplayEnvelope{
		ModuleID:     "renderer",
		RowID:        "render",
		Kind:         "wasm",
		ABI:          ABI,
		Entry:        DefaultEntry,
		Digest:       "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InputHash:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Outcome:      ReplayOutcomeSuccess,
		OutputHash:   "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		FuelConsumed: 10,
		Limits: ReplayLimits{
			Fuel:        100,
			MemoryPages: 1,
			OutputBytes: 64,
		},
		Engine: "wasmtime-go:v46.0.0",
		Arch:   "arm64",
	}
	tests := []struct {
		name      string
		mutate    func(*ReplayEnvelope)
		wantKind  ReplayFindingKind
		wantField string
	}{
		{
			name: "identity",
			mutate: func(env *ReplayEnvelope) {
				env.InputHash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
			},
			wantKind:  ReplayFindingIdentityDivergence,
			wantField: "input_hash",
		},
		{
			name: "profile",
			mutate: func(env *ReplayEnvelope) {
				env.Engine = "wasmtime-go:v47.0.0"
			},
			wantKind:  ReplayFindingUnsupportedProfile,
			wantField: "engine",
		},
		{
			name: "result",
			mutate: func(env *ReplayEnvelope) {
				env.OutputHash = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			},
			wantKind:  ReplayFindingResultDivergence,
			wantField: "output_hash",
		},
		{
			name: "resource",
			mutate: func(env *ReplayEnvelope) {
				env.FuelConsumed++
			},
			wantKind:  ReplayFindingResourceDivergence,
			wantField: "fuel_consumed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := base
			tc.mutate(&actual)
			finding := CompareReplayEnvelopes(base, actual)
			if finding == nil || finding.Kind != tc.wantKind || finding.Field != tc.wantField {
				t.Fatalf("finding = %#v, want %s on %s", finding, tc.wantKind, tc.wantField)
			}
		})
	}
}

func TestCompareReplayEnvelopesTreatsPythonFuelAsEvidenceOnly(t *testing.T) {
	expected := ReplayEnvelope{
		ModuleID:     "python_renderer",
		RowID:        "render",
		Kind:         "python",
		ABI:          "python-json-v1",
		Entry:        "handle",
		Digest:       "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InputHash:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Outcome:      ReplayOutcomeSuccess,
		OutputHash:   "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		FuelConsumed: 10,
		Limits: ReplayLimits{
			Fuel:        100,
			MemoryPages: 8192,
			OutputBytes: 4096,
		},
		Engine:            "wasmtime-go:v46.0.0",
		Arch:              "arm64",
		Interpreter:       "cpython-wasi",
		InterpreterDigest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		SnapshotDigest:    "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		HarnessABI:        "swarm-python-json-v1",
		SourceHash:        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}
	actual := expected
	actual.FuelConsumed++
	if finding := CompareReplayEnvelopes(expected, actual); finding != nil {
		t.Fatalf("CompareReplayEnvelopes finding = %#v, want nil for python fuel drift", finding)
	}
}
