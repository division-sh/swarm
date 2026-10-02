package cliapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenariodocument"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestScenarioInvalidVariantsRequireValidBaseAndTypedRejection(t *testing.T) {
	bundle := generatedInputFixtureBundle(t)
	runner := scenarioRunner{bundle: bundle, source: semanticview.Wrap(bundle)}
	evaluator := mustScenarioExpressionEvaluatorWithSeed(t, "2532")
	file := scenarioTestFile{Path: "alpha/tests/probe.yaml", FlowID: "alpha"}
	valid := map[string]any{"alpha": "000000000000", "id": "00000000-0000-0000-0000-000000000000"}
	for _, row := range []struct {
		name, event string
		payload     any
		set         map[string]any
		pass        bool
	}{
		{"schema rejection", "shared.received", valid, map[string]any{"alpha": int64(7)}, true},
		{"null removes required field", "shared.received", valid, map[string]any{"alpha": nil}, true},
		{"unchanged payload", "shared.received", valid, nil, false},
		{"unknown event", "unknown.received", valid, nil, false},
		{"invalid base", "shared.received", map[string]any{"alpha": int64(7)}, map[string]any{"alpha": nil}, false},
		{"missing fixture", "shared.received", map[string]any{"from": "missing.yaml"}, nil, false},
		{"base CEL failure", "shared.received", map[string]any{"alpha": "${unknown()}", "id": valid["id"]}, nil, false},
		{"override CEL failure", "shared.received", valid, map[string]any{"alpha": "${unknown()}"}, false},
		{"unsafe materialization", "shared.received", valid, map[string]any{"alpha": "${9007199254740993}"}, false},
		{"unsupported CEL result", "shared.received", valid, map[string]any{"alpha": "${b'abc'}"}, false},
		{"colliding CEL keys", "shared.received", valid, map[string]any{"alpha": "${{1:'number','1':'text'}}"}, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			doc := scenarioDocument{Invalid: &scenarioInvalid{Base: map[string]any{"publish": row.event, "payload": row.payload}, Cases: []scenarioInvalidCase{{Name: row.name, Set: row.set}}}}
			err := runner.runInvalidVariants(file, doc, evaluator)
			if (err == nil) != row.pass {
				t.Fatalf("pass=%v error=%v", row.pass, err)
			}
			if row.pass {
				step, _ := invalidBasePublishStep(doc.Invalid.Base)
				payload := cloneAnyMap(valid)
				for key, value := range row.set {
					if err := scenariodocument.SetPath(payload, key, value); err != nil {
						t.Fatal(err)
					}
				}
				step.Payload, _ = scenariodocument.Materialize(payload)
				_, _, err = runner.buildPublishPayload(file, evaluator, step)
				var violation *eventschema.Violation
				if !errors.As(err, &violation) {
					t.Fatalf("not schema-owned: %v", err)
				}
			}
		})
	}
}

func TestScenarioFixturePreparationUsesArtifactAndSingleEvaluation(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tests"), 0o755); err != nil {
		t.Fatal(err)
	}
	for label, raw := range map[string]string{
		"fixture.yaml": "id: \"${'${1 + 1}'}\"\nvalue: '${vars.n + 1}'\n",
		"fixture.json": `{"id":"${'${1 + 1}'}","value":"${vars.n + 1}"}`,
		"bad.yaml":     "id: '${unavailable()}'\n",
		"cycle.yaml":   "id: &a [*a]\n",
	} {
		if err := os.WriteFile(filepath.Join(root, "tests", label), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	runner := scenarioRunner{bundle: &runtimecontracts.WorkflowContractBundle{SourceArtifact: artifact}}
	evaluator, err := newScenarioExpressionEvaluator(scenariodocument.Seed("tests/probe.yaml", "fixture", "fixed"), map[string]any{"n": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	file := scenarioTestFile{Path: "tests/probe.yaml"}
	want := map[string]any{"id": "${1 + 1}", "value": int64(2)}
	for _, spec := range []any{
		map[string]any{"from": "fixture.yaml"}, map[string]any{"from": "fixture.json"},
		map[string]any{"id": "${'${1 + 1}'}", "value": "${vars.n + 1}"},
		map[string]any{"set": map[string]any{"id": "${'${1 + 1}'}", "value": "${vars.n + 1}"}},
		"${{'id':'${1 + 1}','value':vars.n + 1}}",
	} {
		got, err := runner.buildPayloadFromSpec(file, evaluator, spec)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%#v -> %#v, %v", spec, got, err)
		}
	}
	for _, label := range []string{"bad.yaml", "cycle.yaml", "missing.yaml", "../../outside.yaml"} {
		if _, err := runner.buildPayloadFromSpec(file, evaluator, map[string]any{"from": label}); err == nil {
			t.Fatalf("accepted %s", label)
		}
	}
}

func TestScenarioAdmissionRefusesBeforeSessionAcquisition(t *testing.T) {
	for _, raw := range []string{
		"version: 1\nsteps: [{publish: item.received, payload: {item_id: x}}]\n",
		"steps: [{publish: item.received, payload: {item_id: x}}]\nconnector_responses: {}\n",
		"steps: [{publish: item.received, payload: {item_id: x}}]\n---\nsteps: []\n",
		"steps: [{publish: item.received, payload: {item_id: x}}, {mailbox.decide: {match: {anchor_kind: stage_gate}, verdict: '${unknown()}'}}]\n",
		"steps: [{publish: item.received, payload: {item_id: x}}]\nexpect: {entities: [{type: default, fields: '${unknown()}'}]}\n",
		"steps: [{publish: item.received, payload: {item_id: x}}]\ninvalid: {base: {publish: unknown, payload: {}}, cases: [{set: {item_id: null}}]}\n",
		"vars: {choice: \"${{1:'number','1':'text'}}\"}\nsteps: [{publish: item.received, payload: {item_id: \"${vars.choice['1']}\"}}]\n",
		"steps: [{publish: item.received, payload: {item_id: \"${b'abc'}\"}}]\n",
		"steps: [{publish: item.received, payload: {set: {item_id: \"${b'abc'}\"}}}]\n",
		"steps: [{publish: item.received, payload: {from: bad-result.yaml}}]\n",
	} {
		t.Run(raw, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			sourceRoot := writeScenarioRunnerFixture(t)
			writeWorkflowValidationFixtureFile(t, filepath.Join(sourceRoot, "tests", "bad-result.yaml"), "item_id: \"${b'abc'}\"\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(sourceRoot, "tests", "negative.yaml"), raw)
			acquired := false
			opts := defaultRootCommandOptions()
			opts.runTest = func(context.Context, TestSessionRequest, func(context.Context, TestSessionEndpoint) error) error {
				acquired = true
				return errors.New("must not acquire")
			}
			var out, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"test", sourceRoot, "tests/negative.yaml"}, &out, &stderr, opts)
			if code != scenarioTestExitValidation || acquired {
				t.Fatalf("code=%d acquired=%v stderr=%s", code, acquired, stderr.String())
			}
		})
	}
}

func TestScenarioPreparationRetainsExactKeysAndRejectsCollisions(t *testing.T) {
	bundle := generatedInputFixtureBundle(t)
	runner := scenarioRunner{bundle: bundle, source: semanticview.Wrap(bundle)}
	file := scenarioTestFile{Path: "alpha/tests/values.yaml", FlowID: "alpha"}
	for _, expression := range []string{"${{1:'000000000000','1':'111111111111'}}", "${{true:'000000000000','true':'111111111111'}}"} {
		file.Raw = []byte("name: fixed\nseed: fixed\nvars: {choice: \"" + expression + "\"}\nsteps: [{publish: shared.received, payload: {alpha: \"${vars.choice['1']}\", id: 00000000-0000-0000-0000-000000000000}}]\n")
		if _, err := runner.prepareScenario(file); err == nil || !strings.Contains(err.Error(), "object keys must be text") {
			t.Fatalf("collision silently projected: %v", err)
		}
	}
	file.Raw = []byte("name: fixed\nseed: fixed\nvars: {choice: \"${{'1':'000000000000','true':'111111111111',' spaced ':'${1+1}'}}\"}\nsteps: [{publish: shared.received, payload: {alpha: \"${vars.choice['1']}\", id: 00000000-0000-0000-0000-000000000000}}]\n")
	want := map[string]any{"alpha": "000000000000", "id": "00000000-0000-0000-0000-000000000000"}
	for i := 0; i < 100; i++ {
		prepared, err := runner.prepareScenario(file)
		if err != nil {
			t.Fatal(err)
		}
		payload, _, err := runner.buildPublishPayload(file, prepared.evaluator, prepared.document.Steps[0])
		if err != nil || !reflect.DeepEqual(payload, want) {
			t.Fatalf("schema-valid preparation %d: %#v, %v", i, payload, err)
		}
	}
}

func TestScenarioCLIAndDerivedProjectionHaveSameAdmission(t *testing.T) {
	base := "name: profile\nseed: fixed\nvars: {marker: \"${'${1 + 1}'}\"}\nderive: {flow: '.', input: request, payload: {generate: true, set: {id: '${vars.marker}', nested: \"${{'items':['${2+2}',7.0]}}\"}}}\nconnector_responses: {tool: \"${{'value':vars.marker}}\"}\n"
	for _, addition := range []string{"", "version: 1\n", "expect: 7\n", "setup: 7\n", "vars: null\n", "unknown: null\n", "---\nname: other\n"} {
		_, cliErr := parseScenarioDocument([]byte(base + addition))
		declaration, found, deriveErr := scenarioderivation.ParseDeclaration([]byte(base+addition), "tests/parity.yaml")
		if (cliErr == nil) != (deriveErr == nil) {
			t.Fatalf("verdict split: CLI=%v derived=%v", cliErr, deriveErr)
		}
		if addition == "" {
			if !found || declaration.Set["id"] != "${1 + 1}" || !strings.Contains(string(declaration.ConnectorResponses["tool"]), "${1 + 1}") {
				t.Fatalf("rescanned derivation: %#v", declaration)
			}
		} else if cliErr == nil {
			t.Fatalf("accepted %s", addition)
		}
	}
}

func TestScenarioExplicitEmptyExactIsAnAssertion(t *testing.T) {
	doc, err := parseScenarioDocument([]byte("steps: [{publish: request, payload: {}}]\nexpect: {events: {exact: []}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Expect.Empty() {
		t.Fatal("exact presence erased")
	}
	if err := assertScenarioEventExpectations([]string{"unexpected.event"}, doc.Expect.Events); err == nil {
		t.Fatal("unexpected event accepted")
	}
	if err := assertScenarioEventExpectations(nil, doc.Expect.Events); err != nil {
		t.Fatal(err)
	}
}

func TestScenarioAssertionsUseCanonicalSemanticValues(t *testing.T) {
	if err := assertScenarioJSONEqual("safe numeric projection", map[string]any{"value": int64(9007199254740991)}, map[string]any{"value": float64(9007199254740991)}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{int64(9007199254740993), make(chan int)} {
		if err := assertScenarioJSONEqual("unsafe actual", value, float64(9007199254740992)); err == nil {
			t.Fatalf("assertion normalized invalid actual value %T", value)
		}
		if err := assertScenarioJSONEqual("unsafe expected", float64(9007199254740992), value); err == nil {
			t.Fatalf("assertion normalized invalid expected value %T", value)
		}
	}
}
