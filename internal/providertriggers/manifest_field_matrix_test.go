package providertriggers

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

const triggerAllFieldsFixture = `provider: github
payload_object_required: false
payload_object_error: invalid object
payload_source: form
secret: {required: true}
signature:
  type: hmac_sha256
  header: X-Signature
  encoding: base64
  prefix: prefix=
  signed_payload: timestamp_dot_raw_body
  signature_param: v1
  missing_error: signature missing
  invalid_error: signature invalid
  timestamp: {header: X-Time, tolerance: 5m, missing_error: time missing, invalid_error: time invalid, stale_error: time stale}
challenge:
  when: {json_path: '$.kind', equals: challenge, normalize: false, missing_error: kind missing, mismatch_error: kind mismatch}
  response: {json_path: '$.challenge', content_type: text/plain, status: 200, missing_error: challenge missing}
delivery_condition: {json_path: '$.kind', equals: delivery, normalize: false, missing_error: kind missing, mismatch_error: kind mismatch}
delivery_id: {header: X-Delivery, required: true, missing_error: id missing}
event_type: {header: X-Event, required: true, missing_error: type missing}
event_name: {literal: inbound.github.raw}
normalized_events:
  - event: inbound.github.observed
    author_subject: {type: chat, field: value}
    fields:
      value:
        from: message.value
        schema: {type: string}
        optional: false
        convert: text_enum_map
        values: {alpha: alpha}
    when: {exists: [message.value], absent: [ignored], equals: {kind: alpha}, one_of: {kind: [alpha]}}
ack: {mode: durable_before_dispatch}
redact_keys: [token]
metadata: {agent: user_agent}
`

const (
	fieldMissing = 1 << iota
	fieldEmptyText
	fieldEmptyMap
	fieldEmptySequence
	fieldInteger
	fieldFalse
)

type triggerFieldCase struct {
	id, path, canonical string
	accepted            int
}

// IDs are the accepted #2487/#2533 denominator, not a fresh census or inferred oracle.
var triggerBodyFieldCases = []triggerFieldCase{
	{"F209", "ack", "{mode: durable_before_dispatch}", fieldMissing | fieldEmptyMap},
	{"F210", "ack.mode", "durable_before_dispatch", fieldMissing},
	{"F211", "challenge", "", fieldMissing},
	{"F212", "challenge.response", "", 0},
	{"F213", "challenge.response.content_type", "text/plain", fieldMissing | fieldEmptyText},
	{"F214", "challenge.response.json_path", "'$.challenge'", 0},
	{"F215", "challenge.response.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F216", "challenge.response.status", "200", fieldMissing | fieldInteger},
	{"F217", "challenge.when", "", 0},
	{"F218", "challenge.when.equals", "challenge", fieldMissing | fieldEmptyText},
	{"F219", "challenge.when.json_path", "'$.kind'", 0},
	{"F220", "challenge.when.mismatch_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F221", "challenge.when.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F222", "challenge.when.normalize", "true", fieldMissing | fieldFalse},
	{"F223", "delivery_condition", "", fieldMissing},
	{"F224", "delivery_condition.equals", "delivery", fieldMissing | fieldEmptyText},
	{"F225", "delivery_condition.json_path", "'$.kind'", 0},
	{"F226", "delivery_condition.mismatch_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F227", "delivery_condition.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F228", "delivery_condition.normalize", "true", fieldMissing | fieldFalse},
	{"F229", "delivery_id", "{literal: receipt, required: true}", fieldMissing | fieldEmptyMap},
	{"F230", "delivery_id.form_param", "DeliveryId", 0},
	{"F231", "delivery_id.header", "X-Delivery", 0},
	{"F232", "delivery_id.json_path", "'$.id'", 0},
	{"F233", "delivery_id.literal", "receipt", 0},
	{"F234", "delivery_id.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F235", "delivery_id.query_param", "receipt", 0},
	{"F236", "delivery_id.required", "true", fieldMissing | fieldFalse},
	{"F237", "event_name", "{literal: inbound.github.raw}", 0},
	{"F238", "event_name.literal", "inbound.github.raw", 0},
	{"F239", "event_name.template", "inbound.github.raw.{event_type}", 0},
	{"F240", "event_type", "{literal: observed, required: true}", fieldMissing | fieldEmptyMap},
	{"F241", "event_type.form_param", "Kind", 0},
	{"F242", "event_type.header", "X-Event", 0},
	{"F243", "event_type.json_path", "'$.kind'", 0},
	{"F244", "event_type.literal", "observed", 0},
	{"F245", "event_type.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F246", "event_type.query_param", "kind", 0},
	{"F247", "event_type.required", "true", fieldMissing | fieldFalse},
	{"F248", "metadata", "{agent: user_agent}", fieldMissing | fieldEmptyMap},
	{"F249", "normalized_events", "", fieldMissing | fieldEmptySequence},
	{"F250", "normalized_events[].author_subject", "{type: chat, field: value}", fieldMissing},
	{"F251", "normalized_events[].author_subject.field", "value", 0},
	{"F252", "normalized_events[].author_subject.type", "chat", 0},
	{"F253", "normalized_events[].event", "inbound.github.observed", 0},
	{"F254", "normalized_events[].fields", "", 0},
	{"F255", "normalized_events[].fields.*.convert", "text_enum_map", 0},
	{"F256", "normalized_events[].fields.*.from", "message.value", 0},
	{"F257", "normalized_events[].fields.*.optional", "true", fieldMissing | fieldFalse},
	{"F258", "normalized_events[].fields.*.schema", "{type: string}", fieldEmptyMap},
	{"F259", "normalized_events[].fields.*.values", "{alpha: alpha}", 0},
	{"F260", "normalized_events[].when", "", fieldMissing | fieldEmptyMap},
	{"F261", "normalized_events[].when.absent", "[ignored]", fieldMissing | fieldEmptySequence},
	{"F262", "normalized_events[].when.equals", "{kind: ' alpha '}", fieldMissing | fieldEmptyMap},
	{"F263", "normalized_events[].when.exists", "[message.value]", fieldMissing | fieldEmptySequence},
	{"F264", "normalized_events[].when.one_of", "{kind: [alpha]}", fieldMissing | fieldEmptyMap},
	{"F265", "payload_object_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F266", "payload_object_required", "true", fieldMissing | fieldFalse},
	{"F267", "payload_source", "form", fieldMissing},
	{"F268", "provider", "github", 0},
	{"F269", "redact_keys", "[token]", fieldMissing | fieldEmptySequence},
	{"F270", "secret", "{required: true}", 0},
	{"F271", "secret.required", "true", 0},
	{"F272", "signature", "", 0},
	{"F273", "signature.encoding", "base64", fieldMissing},
	{"F274", "signature.header", "X-Signature", 0},
	{"F275", "signature.invalid_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F276", "signature.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F277", "signature.prefix", "prefix=", fieldMissing | fieldEmptyText},
	{"F278", "signature.signature_param", "v1", fieldMissing | fieldEmptyText},
	{"F279", "signature.signed_payload", "timestamp_dot_raw_body", 0},
	{"F280", "signature.timestamp", "", fieldEmptyMap},
	{"F281", "signature.timestamp.header", "X-Time", fieldMissing | fieldEmptyText},
	{"F282", "signature.timestamp.invalid_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F283", "signature.timestamp.missing_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F284", "signature.timestamp.param", "t", fieldMissing | fieldEmptyText},
	{"F285", "signature.timestamp.stale_error", "diagnostic", fieldMissing | fieldEmptyText},
	{"F286", "signature.timestamp.tolerance", "5m", fieldMissing | fieldEmptyText},
	{"F287", "signature.type", "hmac_sha256", 0},
}

func TestTriggerBodyFieldAdmissionMatrix(t *testing.T) {
	if len(triggerBodyFieldCases) != 79 {
		t.Fatal("BODY denominator changed")
	}
	for _, field := range triggerBodyFieldCases {
		t.Run(field.id+"/"+field.path, func(t *testing.T) {
			base := triggerFieldBase(t, field)
			canonical := triggerFieldValue(t, base, field.path)
			if field.canonical != "" {
				if err := yaml.Unmarshal([]byte(field.canonical), &canonical); err != nil {
					t.Fatal(err)
				}
			}
			for _, shape := range []struct {
				name  string
				value any
				flag  int
			}{
				{"canonical", canonical, -1}, {"missing", nil, fieldMissing},
				{"null", nil, 0}, {"empty-text", "", fieldEmptyText},
				{"integer", 7, fieldInteger}, {"false", false, fieldFalse},
				{"empty-map", map[string]any{}, fieldEmptyMap}, {"mapping", map[string]any{"not_declared": 1}, 0},
				{"empty-sequence", []any{}, fieldEmptySequence}, {"sequence", []any{7}, 0},
			} {
				t.Run(shape.name, func(t *testing.T) {
					fixture := triggerFieldBase(t, field)
					triggerSetField(t, fixture, field.path, shape.value, shape.name == "missing")
					body, err := yaml.Marshal(fixture)
					if err != nil {
						t.Fatal(err)
					}
					manifest, err := parseManifestAt(body, "field-matrix/trigger.yaml")
					accepted := shape.flag == -1 || field.accepted&shape.flag != 0
					if (err == nil) != accepted {
						t.Fatalf("%s %s accepted=%t, want %t: %v\n%s", field.id, shape.name, err == nil, accepted, err, body)
					}
					if err != nil {
						if manifest.Validate() == nil || !strings.Contains(err.Error(), "field-matrix/trigger.yaml") {
							t.Fatalf("invalid policy/source provenance escaped admission: %v", err)
						}
						return
					}
					outputs := manifest.OutputManifest()
					catalog, err := NewCatalogSnapshot(CatalogEntry{Manifest: manifest, Identity: PackIdentity{ID: "provider.github", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("b", 64), Provenance: "test"}})
					if err != nil {
						t.Fatal(err)
					}
					entry, _ := catalog.EntryByProvider("github")
					plan, err := catalog.CompileAdmission(CompileAdmissionRequest{Alias: "matrix", Provider: "github", SigningSecret: "webhook_signing.github"})
					if err != nil || !reflect.DeepEqual(outputs, plan.Outputs()) || !reflect.DeepEqual(outputs, entry.Manifest.OutputManifest()) {
						t.Fatalf("admitted projection changed through carriers: %v", err)
					}
				})
			}
		})
	}
}

func triggerFieldBase(t testing.TB, field triggerFieldCase) map[string]any {
	t.Helper()
	var base map[string]any
	if err := yaml.Unmarshal([]byte(triggerAllFieldsFixture), &base); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"delivery_id.", "event_type."} {
		leaf := strings.TrimPrefix(field.path, prefix)
		if leaf != field.path && leaf != "required" && leaf != "missing_error" {
			block := base[strings.TrimSuffix(prefix, ".")].(map[string]any)
			delete(block, "header")
			block[leaf] = "source"
		}
	}
	if field.path == "event_name.template" {
		base["event_name"] = map[string]any{"template": "inbound.github.raw.{event_type}"}
	}
	if field.path == "normalized_events[].fields.*.schema" {
		branch := base["normalized_events"].([]any)[0].(map[string]any)
		delete(branch, "author_subject")
		projection := branch["fields"].(map[string]any)["value"].(map[string]any)
		delete(projection, "convert")
		delete(projection, "values")
	}
	if field.path == "signature.timestamp.param" {
		block := base["signature"].(map[string]any)["timestamp"].(map[string]any)
		delete(block, "header")
		block["param"] = "t"
	}
	return base
}

func triggerFieldValue(t testing.TB, base map[string]any, path string) any {
	t.Helper()
	parent, key := triggerFieldParent(t, base, path)
	return parent[key]
}

func triggerSetField(t testing.TB, base map[string]any, path string, value any, remove bool) {
	t.Helper()
	parent, key := triggerFieldParent(t, base, path)
	if remove {
		delete(parent, key)
	} else {
		parent[key] = value
	}
}

func triggerFieldParent(t testing.TB, base map[string]any, path string) (map[string]any, string) {
	t.Helper()
	parts := strings.Split(strings.ReplaceAll(path, "*", "value"), ".")
	current := base
	for _, part := range parts[:len(parts)-1] {
		if strings.HasSuffix(part, "[]") {
			current = current[strings.TrimSuffix(part, "[]")].([]any)[0].(map[string]any)
		} else {
			current = current[part].(map[string]any)
		}
	}
	return current, parts[len(parts)-1]
}

func TestTriggerBodySharedSchemaMatrix(t *testing.T) {
	for index, field := range []struct{ path, schema string }{
		{"additionalProperties", "{type: object, additionalProperties: {type: integer}}"},
		{"description", "{type: string, description: ' exact '}"},
		{"enum", "{type: any, enum: [null, false, 0, ' alpha ', beta]}"},
		{"format", "{type: string, format: date-time}"},
		{"items", "{type: array, items: {type: string}}"},
		{"maxItems", "{type: array, items: {}, maxItems: 2}"},
		{"maxLength", "{type: string, maxLength: 2}"},
		{"maximum", "{type: number, maximum: 2}"},
		{"minItems", "{type: array, items: {}, minItems: 0}"},
		{"minLength", "{type: string, minLength: 0}"},
		{"minimum", "{type: number, minimum: -2}"},
		{"pattern", "{type: string, pattern: '^a+$'}"},
		{"properties", "{type: object, properties: {child: {type: string}}}"},
		{"required", "{type: object, properties: {child: {type: string}}, required: [child]}"},
		{"type", "{type: integer}"},
		{"properties.right.x-swarm-equalTo", "{type: object, properties: {left: {type: string}, right: {type: string, x-swarm-equalTo: left}}}"},
	} {
		t.Run(fmt.Sprintf("F%d", 288+index), func(t *testing.T) {
			var base map[string]any
			if err := yaml.Unmarshal([]byte(field.schema), &base); err != nil {
				t.Fatal(err)
			}
			canonical := triggerFieldValue(t, base, field.path)
			for _, shape := range []struct {
				name  string
				value any
			}{
				{"canonical", canonical}, {"missing", nil}, {"null", nil}, {"empty-text", ""},
				{"integer", 0}, {"false", false}, {"empty-map", map[string]any{}},
				{"mapping", map[string]any{"not_declared": 1}}, {"empty-sequence", []any{}}, {"sequence", []any{7}},
			} {
				t.Run(shape.name, func(t *testing.T) {
					var modified map[string]any
					if err := yaml.Unmarshal([]byte(field.schema), &modified); err != nil {
						t.Fatal(err)
					}
					triggerSetField(t, modified, field.path, shape.value, shape.name == "missing")
					for _, context := range []string{"root", "object", "array"} {
						t.Run(context, func(t *testing.T) {
							var wrapped any = modified
							switch context {
							case "object":
								wrapped = map[string]any{"type": "object", "properties": map[string]any{"nested": modified}}
							case "array":
								wrapped = map[string]any{"type": "array", "items": modified}
							}
							schema, err := yaml.Marshal(wrapped)
							if err != nil {
								t.Fatal(err)
							}
							snapshot, err := yamlsource.Load(schema)
							if err != nil {
								t.Fatal(err)
							}
							admitted, schemaErr := runtimecontracts.AdmitToolInputSchemaValue(snapshot.Document("schema.yaml").Root())
							if shape.name == "canonical" && schemaErr != nil || shape.name == "null" && schemaErr == nil {
								t.Fatalf("shared owner violated canonical/null control: %v", schemaErr)
							}
							fixture := map[string]any{
								"provider": "acme", "event_name": map[string]any{"literal": "inbound.acme"},
								"normalized_events": []any{map[string]any{
									"event": "inbound.acme.observed", "fields": map[string]any{"value": map[string]any{"from": "message.value", "schema": wrapped}},
								}},
							}
							body, err := yaml.Marshal(fixture)
							if err != nil {
								t.Fatal(err)
							}
							manifest, bodyErr := parseManifestAt(body, "schema-matrix/trigger.yaml")
							if (schemaErr == nil) != (bodyErr == nil) {
								t.Fatalf("BODY admission disagrees with shared schema owner: %v / %v", schemaErr, bodyErr)
							}
							if bodyErr != nil {
								if manifest.Validate() == nil || !strings.Contains(bodyErr.Error(), "schema-matrix/trigger.yaml") {
									t.Fatalf("invalid schema lost source provenance: %v", bodyErr)
								}
								return
							}
							plan := compileTriggerTestPlan(t, manifest)
							want, _ := admitted.Project()
							for _, actual := range []runtimecontracts.ToolInputSchema{manifest.OutputManifest()[1].Fields["value"].Schema, plan.Outputs()[1].Fields["value"].Schema, *manifest.EventCatalogEntries()["inbound.acme.observed"].Payload.Properties["value"].ExactSchema} {
								got, err := actual.Project()
								if err != nil || !reflect.DeepEqual(got, want) {
									t.Fatalf("shared schema authority changed: %#v / %#v, %v", got, want, err)
								}
							}
						})
					}
				})
			}
		})
	}
}
