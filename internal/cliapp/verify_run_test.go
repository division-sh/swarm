package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestVerifyRunPublicFlagAndAbsentStoreAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, run string
		portable  bool
		want      string
	}{
		{"absent", uuid.NewString(), false, "not_run"},
		{"blank", "", false, "failed"},
		{"invalid", "not-a-run", false, "failed"},
		{"portable", uuid.NewString(), true, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := issue2567VerifySource(t)
			stateDir := filepath.Join(t.TempDir(), "never-created")
			args := []string{"verify", root, "--run", tc.run, "--swarm-dir", stateDir, "--json"}
			if tc.portable {
				args = append(args, "--portable")
			}
			var out, errOut bytes.Buffer
			code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut)
			var got verifyRunCommandResult
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatalf("%v: out=%s err=%s", err, &out, &errOut)
			}
			if got.Status != tc.want || got.OK || len(got.Rows) != 0 || got.EntitiesChecked != 0 || (code == 0) != (tc.want == "not_run") {
				t.Fatalf("result=%+v code=%d err=%s", got, code, &errOut)
			}
			if tc.want == "failed" && (len(got.Errors) != 1 || got.Errors[0].Retryable) {
				t.Fatalf("untyped/retryable refusal: %+v", got)
			}
			if tc.want == "not_run" && (!strings.Contains(got.Reason, stateDir) || strings.Contains(out.String(), "no drift")) {
				t.Fatalf("false empty verdict: %s", &out)
			}
			if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
				t.Fatalf("inspection created state: %v", err)
			}
		})
	}
}

func TestVerifyRunTypedFailuresRetainCoordinatesAndCancellation(t *testing.T) {
	run := uuid.NewString()
	for _, tc := range []struct {
		name  string
		cause error
		code  string
		class failures.Class
	}{
		{"missing_run", &runlifecycle.RunNotFoundError{RunID: run}, "run_not_found", failures.ClassSchemaInvalid},
		{"history", &mutationlog.HistoryError{RunID: run, EntityID: "entity", MutationID: "mutation", Code: "mutation_order_missing", Reason: "missing coordinate"}, "mutation_order_missing", failures.ClassSchemaInvalid},
		{"canceled", errors.Join(&runlifecycle.RunNotFoundError{RunID: run}, context.Canceled), "run_inspection_canceled", failures.ClassDependencyUnavailable},
		{"deadline", context.DeadlineExceeded, "run_inspection_timeout", failures.ClassTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure, ok := failures.As(verifyRunFailure(run, tc.cause))
			if !ok || failure.Failure.Class != tc.class || failure.Failure.Detail.Code != tc.code || failure.Failure.Detail.Attributes["run_id"] != run {
				t.Fatalf("wrong disposition: %v", failure)
			}
			if tc.class == failures.ClassSchemaInvalid && failure.Failure.Retryable {
				t.Fatal("deterministic missing evidence was made retryable")
			}
			if tc.name == "history" && (failure.Failure.Detail.Attributes["entity_id"] != "entity" || failure.Failure.Detail.Attributes["mutation_id"] != "mutation") {
				t.Fatal("coordinates lost")
			}
		})
	}
}

func TestVerifyRunPublicOutputParity(t *testing.T) {
	path := "profile.name"
	result := verifyRunCommandResult{DriftReport: mutationlog.DriftReport{RunID: uuid.NewString(), EntitiesChecked: 1, Rows: []mutationlog.DriftRow{{Kind: "value", EntityID: "entity", Domain: mutationlog.DomainAuthoredField, Path: &path, FoldedPresent: true, StoredPresent: true, FoldedValue: int64(7), StoredValue: float64(7), FoldedType: "integer", StoredType: "double"}}}, Status: "drift"}
	var text bytes.Buffer
	if err := writeVerifyRunResult(&text, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "profile.name") || !strings.Contains(text.String(), "integer:7") || !strings.Contains(text.String(), "double:7.0") {
		t.Fatalf("lost evidence: %s", &text)
	}
	wire, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var readback verifyRunCommandResult
	if err := json.Unmarshal(wire, &readback); err != nil {
		t.Fatal(err)
	}
	if readback.Rows[0].FoldedType != "integer" || readback.Rows[0].StoredType != "double" || !readback.Rows[0].StoredPresent {
		t.Fatalf("machine evidence lost: %s", wire)
	}
}
