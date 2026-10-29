package providertriggers

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const triggerPredicateFixture = `event_name: {literal: raw}
delivery_id: {literal: receipt, required: true}
event_type: {literal: observed, required: true}
normalized_events:
  - event: observed
    fields:
      value: {from: message.value, schema: {type: string}}
    when: %s
`

func TestTriggerPredicatePathIdentityBeforeProjection(t *testing.T) {
	for _, predicate := range []string{
		"{equals: {kind: alpha, ' kind ': beta}}",
		"{equals: {' kind ': beta, kind: alpha}}",
		"{equals: {'kind ': alpha}}",
		"{equals: {'kind. value': alpha}}",
		"{equals: {'$.kind': alpha}}",
		"{equals: {1: alpha}}",
		"{exists: [' kind ']}",
		"{absent: ['kind. value']}",
		"{one_of: {' kind ': [alpha]}}",
	} {
		t.Run(predicate, func(t *testing.T) {
			for attempt := 0; attempt < 20; attempt++ {
				manifest, err := admitTestTriggerBody(fmt.Sprintf(triggerPredicateFixture, predicate))
				if err == nil || !strings.Contains(err.Error(), "trigger.yaml:") || !strings.Contains(err.Error(), "when") {
					t.Fatalf("noncanonical predicate admitted: %v", err)
				}
				if manifest.Validate() == nil {
					t.Fatal("invalid source published a usable policy")
				}
			}
		})
	}
	manifest := parseTriggerTestBody(t, fmt.Sprintf(triggerPredicateFixture, "{equals: {kind: alpha, other.kind: beta}}"))
	plan := compileTriggerTestPlan(t, manifest)
	if !reflect.DeepEqual(plan.Outputs()[1].When.Equals, map[string]string{"kind": "alpha", "other.kind": "beta"}) {
		t.Fatalf("distinct paths collapsed: %#v", plan.Outputs())
	}
	for _, matches := range []bool{true, false} {
		kind := "beta"
		if !matches {
			kind = "wrong"
		}
		delivery, err := plan.Accept(Request{Payload: map[string]any{"message": map[string]any{"value": "ok"}, "kind": "alpha", "other": map[string]any{"kind": kind}}})
		if err != nil || (len(delivery.Events) == 2) != matches {
			t.Fatalf("distinct predicates matched incorrectly: %+v, %v", delivery, err)
		}
	}
}

func TestTriggerPredicateExactTextPreservation(t *testing.T) {
	body := []byte(fmt.Sprintf(triggerPredicateFixture, "{equals: {kind: ' alpha '}}"))
	manifest, err := admitTestTriggerBody(string(body))
	if err != nil {
		t.Fatal(err)
	}
	plan := compileTriggerTestPlan(t, manifest)
	for attempt := 0; attempt < 20; attempt++ {
		if got := plan.Outputs()[1].When.Equals["kind"]; got != " alpha " {
			t.Fatalf("literal was normalized: %q", got)
		}
		for _, kind := range []string{" alpha ", "alpha", " alpha", "alpha "} {
			delivery, err := plan.Accept(Request{Payload: map[string]any{"kind": kind, "message": map[string]any{"value": "ok"}}})
			if err != nil || (len(delivery.Events) == 2) != (kind == " alpha ") {
				t.Fatalf("literal matching kind=%q: %+v, %v", kind, delivery, err)
			}
		}
	}
	for _, value := range []string{"''", "null", "1", "false", "{}", "[]"} {
		if _, err := admitTestTriggerBody(fmt.Sprintf(triggerPredicateFixture, "{equals: {kind: "+value+"}}")); err == nil {
			t.Fatalf("invalid equals text admitted: %s", value)
		}
	}
	condition := ConditionManifest{JSONPath: "$.kind", Equals: "Alpha", Normalize: true}
	if matched, err := condition.Evaluate(map[string]any{"kind": " ALPHA "}); err != nil || !matched {
		t.Fatalf("explicit condition normalization was removed: %t, %v", matched, err)
	}
}

const triggerPatternFixture = `event_name: {literal: raw}
delivery_id: {literal: receipt, required: true}
event_type: {literal: observed, required: true}
normalized_events:
  - event: command
    fields:
      command:
        from: message.text
        schema:
          type: object
          additionalProperties: false
          required: [reference]
          properties:
            reference: {type: string}
            address: {type: string}
        convert: {pattern: '%s'}
        optional: %t
`

func TestTriggerPatternConversionAdmissionAndExecution(t *testing.T) {
	pattern := `^/(?P<reference>[A-Za-z0-9_]{1,32})(?:@(?P<address>[A-Za-z0-9_]{5,32}))?$`
	for _, optional := range []bool{false, true} {
		manifest := parseTriggerTestBody(t, fmt.Sprintf(triggerPatternFixture, pattern, optional))
		plan := compileTriggerTestPlan(t, manifest)
		if outputs := plan.Outputs(); outputs[1].Fields["command"].Pattern != pattern || outputs[1].Fields["command"].Convert != "pattern" {
			t.Fatalf("compiled conversion changed: %#v", outputs)
		}
		for _, text := range []string{"/start", "/start@other_bot", "ordinary text", "/start with args"} {
			delivery, err := plan.Accept(Request{Payload: map[string]any{"message": map[string]any{"text": text}}})
			matches := text == "/start" || text == "/start@other_bot"
			if !matches && !optional {
				if err == nil || !strings.Contains(err.Error(), "does not match pattern") || len(delivery.Events) != 0 {
					t.Fatalf("required nonmatch did not fail closed: %+v, %v", delivery, err)
				}
				continue
			}
			if err != nil || len(delivery.Events) != 2 {
				t.Fatalf("pattern execution: %+v, %v", delivery, err)
			}
			command, present := delivery.Events[1].Payload["command"]
			if !matches {
				if present {
					t.Fatalf("optional nonmatch emitted a value: %#v", command)
				}
				continue
			}
			want := map[string]any{"reference": "start"}
			if text == "/start@other_bot" {
				want["address"] = "other_bot"
			}
			if !reflect.DeepEqual(command, want) {
				t.Fatalf("capture projection = %#v, want %#v", command, want)
			}
		}
		if _, err := plan.Accept(Request{Payload: map[string]any{"message": map[string]any{"text": 7}}}); err == nil || !strings.Contains(err.Error(), "pattern requires text") {
			t.Fatalf("non-text pattern input admitted: %v", err)
		}
	}
	for _, pattern := range []string{"[", "(?P<unknown>.+)", "(?P<reference>a)(?P<reference>b)"} {
		if _, err := admitTestTriggerBody(fmt.Sprintf(triggerPatternFixture, pattern, false)); err == nil {
			t.Fatalf("invalid pattern admitted: %q", pattern)
		}
	}
	wrongSchema := strings.Replace(fmt.Sprintf(triggerPatternFixture, pattern, false), "type: object", "type: string", 1)
	if _, err := admitTestTriggerBody(wrongSchema); err == nil {
		t.Fatal("pattern without object schema admitted")
	}
	retired := strings.Replace(fmt.Sprintf(triggerPatternFixture, pattern, false), "{pattern: '"+pattern+"'}", "telegram_command", 1)
	if _, err := admitTestTriggerBody(retired); err == nil || !strings.Contains(err.Error(), "want one of") || strings.Contains(err.Error(), "retired") {
		t.Fatalf("closed conversion enum = %v", err)
	}
}

func TestTriggerPatternAdmissionRejectsNonStringCaptureProperties(t *testing.T) {
	for _, optional := range []bool{false, true} {
		for _, tc := range []struct{ kind, schema string }{
			{"integer", "{type: integer}"},
			{"number", "{type: number}"},
			{"boolean", "{type: boolean}"},
			{"object", "{type: object}"},
			{"array", "{type: array, items: {type: string}}"},
			{"null", "{type: 'null'}"},
			{"any", "{type: any}"},
		} {
			t.Run(fmt.Sprintf("optional=%t/type=%s", optional, tc.kind), func(t *testing.T) {
				body := fmt.Sprintf(triggerPatternFixture, `^(?P<reference>.+)$`, optional)
				body = strings.Replace(body, "reference: {type: string}", "reference: "+tc.schema, 1)
				manifest, err := admitTestTriggerBody(body)
				if err == nil || !strings.Contains(err.Error(), "capture reference requires a string output property") || !strings.Contains(err.Error(), "trigger.yaml:") {
					t.Fatalf("non-string capture schema admitted: %v", err)
				}
				if manifest.Validate() == nil {
					t.Fatal("rejected source published a usable policy")
				}
			})
		}
	}
}

func TestTriggerPatternAdmissionRejectsRequiredPropertyWithoutCapture(t *testing.T) {
	for _, optional := range []bool{false, true} {
		t.Run(fmt.Sprintf("optional=%t", optional), func(t *testing.T) {
			body := fmt.Sprintf(triggerPatternFixture, `^(?P<reference>.+)$`, optional)
			body = strings.Replace(body, "required: [reference]", "required: [reference, address]", 1)
			manifest, err := admitTestTriggerBody(body)
			if err == nil || !strings.Contains(err.Error(), "required output property address has no named capture") || !strings.Contains(err.Error(), "trigger.yaml:") {
				t.Fatalf("unsupplied required property admitted: %v", err)
			}
			if manifest.Validate() == nil {
				t.Fatal("rejected source published a usable policy")
			}
		})
	}
}

func TestTriggerPatternAdmissionPreservesCaptureParticipationAndValueValidation(t *testing.T) {
	for _, optional := range []bool{false, true} {
		t.Run(fmt.Sprintf("optional=%t", optional), func(t *testing.T) {
			body := fmt.Sprintf(triggerPatternFixture, `^/(?P<reference>.*)(?:@(?P<address>[a-z]+))?$`, optional)
			plan := compileTriggerTestPlan(t, parseTriggerTestBody(t, body))
			delivery, err := plan.Accept(Request{Payload: map[string]any{"message": map[string]any{"text": "/"}}})
			if err != nil || len(delivery.Events) != 2 || !reflect.DeepEqual(delivery.Events[1].Payload["command"], map[string]any{"reference": ""}) {
				t.Fatalf("empty participating capture or absent optional capture changed: %+v, %v", delivery, err)
			}

			uncaptured := strings.Replace(body, "address: {type: string}", "address: {type: integer}", 1)
			uncaptured = strings.Replace(uncaptured, `(?:@(?P<address>[a-z]+))?`, "", 1)
			plan = compileTriggerTestPlan(t, parseTriggerTestBody(t, uncaptured))
			if delivery, err := plan.Accept(Request{Payload: map[string]any{"message": map[string]any{"text": "/ok"}}}); err != nil || len(delivery.Events) != 2 {
				t.Fatalf("uncaptured optional property rejected: %+v, %v", delivery, err)
			}

			for _, constrained := range []string{
				strings.Replace(body, "reference: {type: string}", "reference: {type: string, minLength: 2}", 1),
				strings.Replace(body, "required: [reference]", "required: [reference, address]", 1),
			} {
				plan = compileTriggerTestPlan(t, parseTriggerTestBody(t, constrained))
				if delivery, err := plan.Accept(Request{Payload: map[string]any{"message": map[string]any{"text": "/x"}}}); err == nil || len(delivery.Events) != 0 {
					t.Fatalf("per-delivery schema rejection weakened: %+v, %v", delivery, err)
				}
			}
		})
	}
}

func TestTriggerBodyHostileSourceProvenance(t *testing.T) {
	for _, suffix := range []string{
		"\nunknown: null\n", "\nprovider: other\n", "\n---\n{}\n",
		"\n<<: {unknown: null}\n", "\nack: &a {mode: unknown}\n",
		"\nsecret: &cycle {required: *cycle}\n",
	} {
		_, err := parseManifestAt([]byte("provider: acme\nevent_name: {literal: inbound.acme}\n"+suffix), "source/provider/trigger.yaml")
		if err == nil || !strings.Contains(err.Error(), "source/provider/trigger.yaml") {
			t.Fatalf("hostile BODY or provenance admitted: %q: %v", suffix, err)
		}
	}
	manifest := parseTriggerTestBody(t, "event_name: &event {literal: raw}\nmetadata: &facts {value: user_agent}\nack: &policy {mode: durable_before_dispatch}\n<<: {secret: {required: false}}\n")
	if got := manifest.Metadata()["value"]; got != "user_agent" {
		t.Fatalf("faithful merge lost its data: %q", got)
	}
}

func parseTriggerTestBody(t testing.TB, body string) Manifest {
	t.Helper()
	manifest, err := admitTestTriggerBody(body)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func admitTestTriggerBody(body string) (Manifest, error) {
	source, err := yamlsource.Load([]byte(body))
	if err != nil {
		return Manifest{}, err
	}
	definition, err := admitTriggerDefinition(source.Document("trigger.yaml").Root())
	if err != nil {
		return Manifest{}, err
	}
	metadata, err := triggerFixture(definition).metadata()
	if err != nil {
		return Manifest{}, err
	}
	provider, err := s1LoadProviderMembers("acme", map[string][]byte{
		"manifest.yaml": metadata,
		"trigger.yaml":  []byte(body),
	})
	if err != nil {
		return Manifest{}, err
	}
	trigger, _ := provider.Body(packmodel.BodyTrigger)
	return AdmitBody(trigger)
}

func compileTriggerTestPlan(t testing.TB, manifest Manifest) InboundAdmissionPlan {
	t.Helper()
	catalog, err := NewCatalogSnapshotFromAdmittedBodies(manifest)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "observed", Provider: manifest.Provider()})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestTriggerBodyDefaultAlternativesUseNormalEnumErrors(t *testing.T) {
	for _, authored := range []string{"payload_source: payload", "ack: {mode: after_publish}", "secret: {required: true}\nsignature: {type: hmac_sha256, header: X-Signature, signed_payload: raw_body, encoding: hex}"} {
		_, err := ParseManifest([]byte("provider: acme\nevent_name: {literal: inbound.acme}\n" + authored + "\n"))
		if err == nil || !strings.Contains(err.Error(), "want one of") || strings.Contains(err.Error(), "retired") || strings.Contains(err.Error(), "migration") {
			t.Fatalf("ordinary enum admission missing: %v", err)
		}
	}
	for _, field := range []string{"encoding", "prefix", "signed_payload", "signature_param", "timestamp"} {
		for _, value := range []string{"null", "''", "{}", "[]", "false", "7"} {
			body := "provider: acme\nevent_name: {literal: inbound.acme}\nsecret: {required: true}\nsignature: {type: token_equality, header: X-Secret, " + field + ": " + value + "}\n"
			_, err := ParseManifest([]byte(body))
			if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); !ok || !strings.Contains(diagnostic.Location.YAMLPath, field) {
				t.Fatalf("inactive field did not use closed-field admission: %s: %v", body, err)
			}
		}
	}
	manifest := parseTriggerTestBody(t, "event_name: {literal: raw}\ndelivery_id: {literal: receipt}\nevent_type: {literal: observed}\n")
	delivery, err := manifest.Accept(Request{Headers: http.Header{}, Payload: map[string]any{}})
	if err != nil || delivery.AcknowledgeBeforeDispatch {
		t.Fatalf("omitted default changed execution: %+v, %v", delivery, err)
	}
}
