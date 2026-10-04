package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestVerifyNamesOnlyRootInterfaceHasNoHarnessClassification(t *testing.T) {
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress)
	var stdout, stderr bytes.Buffer
	if code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr); code != 0 {
		t.Fatalf("verify exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "validation: structural; live readiness: not evaluated") || strings.Contains(stdout.String(), "harness") || strings.Contains(stdout.String(), "production-valid") {
		t.Fatalf("verify retains retired classification: %s", stdout.String())
	}
	opts := defaultVerifyCommandOptions()
	opts.sourceRoot, opts.configPath, opts.output.asJSON = root, writeTestVerifyRuntimeConfig(t), true
	stdout.Reset()
	stderr.Reset()
	if code := runVerifyCommandWithOutput(context.Background(), RepoRoot(), opts, &stdout, &stderr); code != 0 {
		t.Fatalf("verify JSON exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var output verifyCommandResult
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if !output.OK || output.BundleHash == "" || output.ValidationScope != "structural" || output.LiveReadiness != "not_evaluated" {
		t.Fatalf("root interface evidence missing: %#v", output)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"harness_injected_inputs", "harness_observed_outputs", "harness_input_provenance", "harness_output_provenance", "production_valid"} {
		if _, present := fields[retired]; present {
			t.Fatalf("verify JSON retains %q: %s", retired, stdout.Bytes())
		}
	}
}

func TestVerifyRejectsRetiredHarnessMarkersAtSourceAdmission(t *testing.T) {
	for _, input := range []bool{false, true} {
		t.Run(map[bool]string{false: "output", true: "input"}[input], func(t *testing.T) {
			root := canonicalrouting.CopyRetiredPinMarker(t, input)
			var stdout, stderr bytes.Buffer
			code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
			want := "must be a scalar text"
			if input {
				want = "field \"source\" is not supported"
			}
			if code == 0 || !strings.Contains(stderr.String(), want) || strings.Contains(stdout.String(), "verify ok") {
				t.Fatalf("retired marker admitted: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}
