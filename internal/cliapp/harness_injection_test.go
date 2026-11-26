package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestVerifyNamesOnlyRootInterfaceHasNoHarnessClassification(t *testing.T) {
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress)
	var stdout, stderr bytes.Buffer
	if code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr); code != 0 {
		t.Fatalf("verify exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "portable structural checks passed; deployment admission not evaluated") || strings.Contains(stdout.String(), "harness") || strings.Contains(stdout.String(), "production-valid") {
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
	t.Run("captured_root_interface_is_frozen", func(t *testing.T) {
		privateRoot := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
		schemaFile := filepath.Join(privateRoot, "schema.yaml")
		schema, err := os.ReadFile(schemaFile)
		if err != nil {
			t.Fatal(err)
		}
		writeDescribeTestFile(t, schemaFile, string(schema)+"\noutputs: [item.processed]\n")
		_, original, err := NewSwarmWorkflowModule(RepoRoot(), privateRoot)
		if err != nil {
			t.Fatal(err)
		}
		rooted, err := original.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		interfaceOf := func(bundle *contracts.WorkflowContractBundle) [4]string {
			t.Helper()
			source := semanticview.Wrap(bundle)
			inputs, outputs := source.FlowInputEventPins("."), source.FlowOutputEventPins(".")
			if len(inputs) != 1 || len(outputs) != 1 {
				t.Fatalf("captured root lost its exact pin set: inputs=%+v outputs=%+v", inputs, outputs)
			}
			input, publicInput := semanticview.SelectedRootInputPin(source, inputs[0].EventType())
			output, publicOutput := semanticview.SelectedRootOutputPin(source, outputs[0].EventType())
			receiver, hasInputSchema := input.ReceiverEventSchema()
			producer, hasOutputSchema := output.EventSchema()
			if !publicInput || !publicOutput || !hasInputSchema || !hasOutputSchema || len(semanticview.SelectedRootInputEndpoints(source)) != 1 {
				t.Fatal("compiled public interface lost a declared endpoint or schema")
			}
			return [4]string{input.EventType(), output.EventType(), receiver.AcceptanceSchemaDigest(), producer.AcceptanceSchemaDigest()}
		}
		before := interfaceOf(original)
		writeDescribeTestFile(t, schemaFile, "stages: []\ninputs: [renamed.received]\noutputs: [renamed.processed]\n")
		writeDescribeTestFile(t, filepath.Join(privateRoot, "events.yaml"), "renamed.received:\n  item_id: integer\nrenamed.processed:\n  item_id: integer\n")
		writeDescribeTestFile(t, filepath.Join(privateRoot, "nodes.yaml"), "updated:\n  renamed.received:\n    emit:\n      event: renamed.processed\n      fields: {item_id: payload.item_id}\n")
		_, changed, err := NewSwarmWorkflowModule(RepoRoot(), privateRoot)
		if err != nil {
			t.Fatal(err)
		}
		after := interfaceOf(changed)
		for i := range before {
			if before[i] == after[i] {
				t.Fatalf("private mutation did not change pin/schema coordinate %d: before=%+v after=%+v", i, before, after)
			}
		}
		retained, err := contracts.LoadWorkflowContractBundleFromRootedSource(RepoRoot(), rooted, contracts.DefaultPlatformSpecFile(RepoRoot()), contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := interfaceOf(retained); got != before || retained.SourceArtifact.BundleHash() != original.SourceArtifact.BundleHash() {
			t.Fatalf("retained public interface changed with the working tree: got=%+v want=%+v", got, before)
		}
	})
}

func TestVerifyRejectsRetiredHarnessMarkersAtSourceAdmission(t *testing.T) {
	for _, input := range []bool{false, true} {
		t.Run(map[bool]string{false: "output", true: "input"}[input], func(t *testing.T) {
			root := canonicalrouting.CopyRetiredPinMarker(t, input)
			var stdout, stderr bytes.Buffer
			code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
			want := "must be a scalar text"
			if code == 0 || !strings.Contains(stderr.String(), want) || strings.Contains(stdout.String(), "verify ok") {
				t.Fatalf("retired marker admitted: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}
