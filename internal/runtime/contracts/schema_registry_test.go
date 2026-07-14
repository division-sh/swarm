package contracts

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"gopkg.in/yaml.v3"
)

func TestEventSchemaRegistryFromCatalog_NormalizesAnnotatedFieldTypes(t *testing.T) {
	registry := EventSchemaRegistryFromCatalog(map[string]EventCatalogEntry{
		"scan.requested": {
			Payload: EventPayloadSpec{
				Properties: map[string]EventFieldSpec{
					"corpus_path":   {Type: "string (required for corpus mode — path to signals file in /data)"},
					"finished_at":   {Type: "timestamp (nullable)"},
					"score":         {Type: "float"},
					"subcategories": {Type: "array (required for saas_gap, local_services modes)"},
				},
			},
		},
	})

	schema, ok := registry["scan.requested"]
	if !ok {
		t.Fatal("missing schema")
	}
	props, _ := schema.Schema["properties"].(map[string]any)
	corpusPath, _ := props["corpus_path"].(map[string]any)
	if corpusPath["type"] != "string" {
		t.Fatalf("corpus_path type = %#v", corpusPath["type"])
	}
	if corpusPath["description"] == "" {
		t.Fatalf("corpus_path description = %#v", corpusPath["description"])
	}
	finishedAt, _ := props["finished_at"].(map[string]any)
	if finishedAt["type"] != "string" || finishedAt["format"] != "date-time" || finishedAt["nullable"] != true {
		t.Fatalf("finished_at schema = %#v, want nullable date-time string", finishedAt)
	}
	score, _ := props["score"].(map[string]any)
	if score["type"] != "number" {
		t.Fatalf("score type = %#v", score["type"])
	}
	subcategories, _ := props["subcategories"].(map[string]any)
	if subcategories["type"] != "array" {
		t.Fatalf("subcategories type = %#v", subcategories["type"])
	}
}

func TestEventSchemaRegistryFromCatalog_ProjectsSchemaRefinements(t *testing.T) {
	minLength := 40
	maxLength := 40
	minScore := 0.0
	maxScore := 1.0
	minFiles := 1
	schema := eventSchemaFromCatalogEntry("deploy.requested", EventCatalogEntry{
		Payload: EventPayloadSpec{
			Properties: map[string]EventFieldSpec{
				"source_ref": {
					Type: "text",
					Refinements: SchemaRefinements{
						Pattern: "^[0-9a-f]{40}$",
						Length:  SchemaLengthRefinement{Min: &minLength, Max: &maxLength},
					},
				},
				"source_repo_url": {Type: "text"},
				"score": {
					Type:        "numeric",
					Refinements: SchemaRefinements{Range: SchemaRangeRefinement{Min: &minScore, Max: &maxScore}},
				},
				"file_manifest": {
					Type:        "[text]",
					Refinements: SchemaRefinements{Length: SchemaLengthRefinement{Min: &minFiles}},
				},
				"component": {Type: "text"},
				"owner": {
					Type:        "text",
					Refinements: SchemaRefinements{EqualTo: "component"},
				},
			},
			Required: []string{"source_ref", "source_repo_url", "file_manifest", "score", "component", "owner"},
		},
	}, TypeCatalogDocument{})

	props, _ := schema.Schema["properties"].(map[string]any)
	sourceRef, _ := props["source_ref"].(map[string]any)
	if sourceRef["pattern"] != "^[0-9a-f]{40}$" || sourceRef["minLength"] != 40 || sourceRef["maxLength"] != 40 {
		t.Fatalf("source_ref refinements = %#v", sourceRef)
	}
	score, _ := props["score"].(map[string]any)
	if score["minimum"] != 0.0 || score["maximum"] != 1.0 {
		t.Fatalf("score refinements = %#v", score)
	}
	fileManifest, _ := props["file_manifest"].(map[string]any)
	if fileManifest["minItems"] != 1 {
		t.Fatalf("file_manifest refinements = %#v", fileManifest)
	}
	owner, _ := props["owner"].(map[string]any)
	if owner["x-swarm-equalTo"] != "component" {
		t.Fatalf("owner refinements = %#v", owner)
	}
}

func TestPlatformEventCatalogImplicitRequiredSkipsNullableFields(t *testing.T) {
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(`
payload:
  required_value: string
  optional_value: string (nullable)
  explicitly_optional_value: string (optional; producer may omit it)
`), &node); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}

	entry := platformEventEntryFromYAMLNode(*node.Content[0])

	if len(entry.Required) != 1 || entry.Required[0] != "required_value" {
		t.Fatalf("Required = %#v, want only required_value", entry.Required)
	}
}

func TestPlatformEventCatalogSchemasValidateCurrentProducerPayloadShapes(t *testing.T) {
	registry := EventSchemaRegistryFromBundle(&WorkflowContractBundle{
		Platform: currentPlatformSpecForSchemaRegistryTest(t),
	})

	for _, tc := range []struct {
		eventType string
		payload   map[string]any
	}{
		{
			eventType: "platform.inbound_recorded",
			payload: map[string]any{
				"provider":          "github",
				"provider_event_id": "provider-evt-1",
				"entity_id":         "00000000-0000-0000-0000-000000000127",
			},
		},
		{
			eventType: "platform.activity_requested",
			payload: map[string]any{
				"activity_id": "telegram_send_message", "tool": "telegram.send_message",
				"input":        map[string]any{"chat_id": float64(42), "text": "hello"},
				"effect_class": "non_idempotent_write", "success_event": "telegram-chat.telegram_send_message.succeeded",
				"failure_event": "telegram-chat.telegram_send_message.failed", "retry_max_attempts": 1,
				"retry_backoff": "none", "fork_policy": "reuse_recorded_result",
				"entity_id": "00000000-0000-0000-0000-000000000127", "node_id": "telegram-responder",
				"flow_id": "telegram-chat", "handler_event_key": "inbound.telegram",
				"source_event_id": "00000000-0000-0000-0000-000000000128",
				"source_run_id":   "00000000-0000-0000-0000-000000000129", "source_task_id": "",
				"parent_event_id": "00000000-0000-0000-0000-000000000128", "chain_depth": 1,
				"attempt": 1, "loop_generation": map[string]any{},
			},
		},
		{
			eventType: "platform.auth_required",
			payload: map[string]any{
				"agent_id":      "agent-a",
				"entity_id":     "00000000-0000-0000-0000-000000000128",
				"flow_instance": "review/inst-1",
				"tool_name":     nil,
				"action":        "llm_call",
				"failure": map[string]any{
					"schema_version": runtimefailures.EnvelopeSchemaVersion,
					"class":          string(runtimefailures.ClassAuthenticationNeeded),
					"detail": map[string]any{
						"code":       "credential_required",
						"attributes": map[string]any{"auth_kind": "provider"},
					},
					"retryable":     false,
					"deterministic": true,
					"message":       "Authentication is required (credential_required).",
					"remediation":   "Provide or refresh the required credential (credential_required).",
					"component":     "manager-test",
					"operation":     "authenticate",
				},
				"timestamp": "2026-06-02T03:00:00Z",
			},
		},
		{
			eventType: "platform.agent_directive",
			payload: map[string]any{
				"operation_id":      "00000000-0000-0000-0000-000000000132",
				"agent_id":          "agent-1",
				"directive_text":    "run corpus",
				"mode":              "directive",
				"run_id":            "00000000-0000-0000-0000-000000000129",
				"run_id_resolution": "specified",
				"source":            "v1_rpc",
				"timestamp":         "2026-06-02T03:00:00Z",
			},
		},
		{
			eventType: "human_task.approved",
			payload: map[string]any{
				"card_id":            "00000000-0000-0000-0000-000000000130",
				"requester_agent_id": "provider-agent",
				"status":             "approved",
				"fields":             map[string]any{},
				"decided_by":         "operator",
				"decided_at":         "2026-07-09T12:00:00Z",
			},
		},
		{
			eventType: "human_task.deferred",
			payload: map[string]any{
				"card_id":   "00000000-0000-0000-0000-000000000131",
				"status":    "deferred",
				"cause":     "operator_deferred",
				"resume_at": "2026-07-09T13:00:00Z",
			},
		},
	} {
		t.Run(tc.eventType, func(t *testing.T) {
			schema, ok := registry[tc.eventType]
			if !ok {
				t.Fatalf("missing generated schema for %s", tc.eventType)
			}
			if err := eventschema.ValidatePayloadAgainstSchema(schema.Schema, tc.payload); err != nil {
				t.Fatalf("generated %s schema rejected producer payload %#v: %v", tc.eventType, tc.payload, err)
			}
		})
	}
}

func TestEventSchemaRegistryFromCatalog_NormalizesPrecisionQualifiedTypeRefsRecursively(t *testing.T) {
	schema := eventSchemaFromCatalogEntry("category.assessed", EventCatalogEntry{
		Payload: EventPayloadSpec{
			Properties: map[string]EventFieldSpec{
				"score":        {Type: "numeric(5,2)"},
				"capabilities": {Type: "RequiredCapabilities"},
				"history":      {Type: "[RequiredCapabilities]"},
			},
		},
	}, TypeCatalogDocument{
		Types: map[string]NamedTypeDecl{
			"RequiredCapabilities": {
				Fields: map[string]TypeFieldSpec{
					"automation_with_unlock": {Type: "numeric(5,2)"},
				},
			},
		},
	})

	props, _ := schema.Schema["properties"].(map[string]any)
	score, _ := props["score"].(map[string]any)
	if got := score["type"]; got != "number" {
		t.Fatalf("score type = %#v, want number", got)
	}
	capabilities, _ := props["capabilities"].(map[string]any)
	capabilityProps, _ := capabilities["properties"].(map[string]any)
	automation, _ := capabilityProps["automation_with_unlock"].(map[string]any)
	if got := automation["type"]; got != "number" {
		t.Fatalf("nested automation type = %#v, want number", got)
	}
	history, _ := props["history"].(map[string]any)
	items, _ := history["items"].(map[string]any)
	itemProps, _ := items["properties"].(map[string]any)
	itemAutomation, _ := itemProps["automation_with_unlock"].(map[string]any)
	if got := itemAutomation["type"]; got != "number" {
		t.Fatalf("list item automation type = %#v, want number", got)
	}
}

func TestEventSchemaRegistryFromBundle_PreservesWave1TypeMeaning(t *testing.T) {
	reviewFlow := FlowContractView{
		Paths: FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Events: map[string]EventCatalogEntry{
			"task.requested": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"mode": {Type: "Mode"},
					},
				},
			},
		},
	}
	registry := EventSchemaRegistryFromBundle(&WorkflowContractBundle{
		RootTypes: TypeCatalogDocument{
			Scalars: map[string]ScalarTypeDecl{
				"URL": {Base: "text"},
			},
			Types: map[string]NamedTypeDecl{
				"ScanDetails": {
					Fields: map[string]TypeFieldSpec{
						"source": {Type: "text"},
						"count":  {Type: "integer"},
					},
				},
			},
		},
		Events: map[string]EventCatalogEntry{
			"scan.completed": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"details":    {Type: "ScanDetails"},
						"urls":       {Type: "[URL]"},
						"trace_id":   {Type: "uuid"},
						"started_at": {Type: "timestamp"},
					},
				},
			},
		},
		FlowTree: FlowTree{
			Root: &FlowContractView{
				Children: []FlowContractView{reviewFlow},
				Events: map[string]EventCatalogEntry{
					"scan.completed": {
						Payload: EventPayloadSpec{
							Properties: map[string]EventFieldSpec{
								"details":    {Type: "ScanDetails"},
								"urls":       {Type: "[URL]"},
								"trace_id":   {Type: "uuid"},
								"started_at": {Type: "timestamp"},
							},
						},
					},
				},
			},
			ByID: map[string]*FlowContractView{
				"review": &reviewFlow,
			},
		},
		flowTypes: map[string]TypeCatalogDocument{
			"review": {
				Enums: map[string]EnumTypeDecl{
					"Mode": {Values: []string{"fast", "deep"}},
				},
			},
		},
	})

	rootSchema, ok := registry["scan.completed"]
	if !ok {
		t.Fatal("missing root schema")
	}
	rootProps, _ := rootSchema.Schema["properties"].(map[string]any)
	details, _ := rootProps["details"].(map[string]any)
	if got := details["type"]; got != "object" {
		t.Fatalf("details type = %#v, want object", got)
	}
	if got := details["additionalProperties"]; got != false {
		t.Fatalf("details additionalProperties = %#v, want false", got)
	}
	detailProps, _ := details["properties"].(map[string]any)
	if _, ok := detailProps["source"].(map[string]any); !ok {
		t.Fatalf("details.properties[source] missing: %#v", detailProps)
	}
	if _, ok := detailProps["count"].(map[string]any); !ok {
		t.Fatalf("details.properties[count] missing: %#v", detailProps)
	}
	urls, _ := rootProps["urls"].(map[string]any)
	if got := urls["type"]; got != "array" {
		t.Fatalf("urls type = %#v, want array", got)
	}
	items, _ := urls["items"].(map[string]any)
	if got := items["type"]; got != "string" {
		t.Fatalf("urls items.type = %#v, want string", got)
	}
	traceID, _ := rootProps["trace_id"].(map[string]any)
	if got := traceID["format"]; got != "uuid" {
		t.Fatalf("trace_id format = %#v, want uuid", got)
	}
	startedAt, _ := rootProps["started_at"].(map[string]any)
	if got := startedAt["format"]; got != "date-time" {
		t.Fatalf("started_at format = %#v, want date-time", got)
	}

	reviewSchema, ok := registry["review/task.requested"]
	if !ok {
		reviewSchema, ok = registry["task.requested"]
	}
	if !ok {
		t.Fatal("missing flow schema")
	}
	reviewProps, _ := reviewSchema.Schema["properties"].(map[string]any)
	mode, _ := reviewProps["mode"].(map[string]any)
	enumValues, _ := mode["enum"].([]any)
	if len(enumValues) != 2 || enumValues[0] != "fast" || enumValues[1] != "deep" {
		t.Fatalf("mode enum = %#v, want [fast deep]", mode["enum"])
	}
}

func TestEventSchemaForFlowEvent_UsesDeclaringFlowTypeCatalogForOverride(t *testing.T) {
	reviewFlow := FlowContractView{
		Paths: FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Events: map[string]EventCatalogEntry{
			"task.requested": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"priority": {Type: "Priority"},
					},
				},
			},
		},
	}
	root := &FlowContractView{
		Events: map[string]EventCatalogEntry{
			"task.requested": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"priority": {Type: "Priority"},
					},
				},
			},
		},
		Children: []FlowContractView{reviewFlow},
	}
	bundle := &WorkflowContractBundle{
		RootTypes: TypeCatalogDocument{
			Enums: map[string]EnumTypeDecl{
				"Priority": {Values: []string{"low"}},
			},
		},
		FlowTree: FlowTree{
			Root: root,
			ByID: map[string]*FlowContractView{
				"review": &root.Children[0],
			},
		},
		flowTypes: map[string]TypeCatalogDocument{
			"review": {
				Enums: map[string]EnumTypeDecl{
					"Priority": {Values: []string{"urgent"}},
				},
			},
		},
	}

	rootSchema, rootKey, ok := EventSchemaForFlowEvent(bundle, "", "task.requested")
	if !ok {
		t.Fatal("missing root event schema")
	}
	if rootKey != "task.requested" {
		t.Fatalf("root key = %q, want task.requested", rootKey)
	}
	rootProps, _ := rootSchema.Schema["properties"].(map[string]any)
	rootPriority, _ := rootProps["priority"].(map[string]any)
	if got := rootPriority["enum"]; len(got.([]any)) != 1 || got.([]any)[0] != "low" {
		t.Fatalf("root priority enum = %#v, want [low]", got)
	}

	reviewSchema, reviewKey, ok := EventSchemaForFlowEvent(bundle, "review", "task.requested")
	if !ok {
		t.Fatal("missing review event schema")
	}
	if reviewKey != "review/task.requested" {
		t.Fatalf("review key = %q, want review/task.requested", reviewKey)
	}
	reviewProps, _ := reviewSchema.Schema["properties"].(map[string]any)
	reviewPriority, _ := reviewProps["priority"].(map[string]any)
	if got := reviewPriority["enum"]; len(got.([]any)) != 1 || got.([]any)[0] != "urgent" {
		t.Fatalf("review priority enum = %#v, want [urgent]", got)
	}

	absoluteSchema, absoluteKey, ok := EventSchemaForFlowEvent(bundle, "", "review/task.requested")
	if !ok {
		t.Fatal("missing absolute review event schema")
	}
	if absoluteKey != "review/task.requested" {
		t.Fatalf("absolute key = %q, want review/task.requested", absoluteKey)
	}
	absoluteProps, _ := absoluteSchema.Schema["properties"].(map[string]any)
	absolutePriority, _ := absoluteProps["priority"].(map[string]any)
	if got := absolutePriority["enum"]; len(got.([]any)) != 1 || got.([]any)[0] != "urgent" {
		t.Fatalf("absolute priority enum = %#v, want [urgent]", got)
	}

	instanceSchema, instanceKey, ok := EventSchemaForFlowEvent(bundle, "review", "review/inst-1/task.requested")
	if !ok {
		t.Fatal("missing instance-scoped review event schema")
	}
	if instanceKey != "review/task.requested" {
		t.Fatalf("instance key = %q, want review/task.requested", instanceKey)
	}
	instanceProps, _ := instanceSchema.Schema["properties"].(map[string]any)
	instancePriority, _ := instanceProps["priority"].(map[string]any)
	if got := instancePriority["enum"]; len(got.([]any)) != 1 || got.([]any)[0] != "urgent" {
		t.Fatalf("instance priority enum = %#v, want [urgent]", got)
	}
}

func TestEventSchemaForFlowEvent_UsesDeclaringRootTypeCatalogForChildOutput(t *testing.T) {
	childFlow := FlowContractView{
		Paths: FlowContractPaths{ID: "child", Flow: "child"},
		Path:  "child",
		Schema: FlowSchemaDocument{
			Pins: FlowPins{
				Outputs: FlowOutputPins{Events: []string{"handoff.completed"}},
			},
		},
	}
	root := &FlowContractView{
		Events: map[string]EventCatalogEntry{
			"handoff.completed": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"evidence": {Type: "Evidence"},
					},
					Required: []string{"evidence"},
				},
			},
		},
		Children: []FlowContractView{childFlow},
	}
	bundle := &WorkflowContractBundle{
		RootTypes: TypeCatalogDocument{
			Types: map[string]NamedTypeDecl{
				"Evidence": {
					Fields: map[string]TypeFieldSpec{
						"root_field": {Type: "text"},
					},
				},
			},
		},
		FlowTree: FlowTree{
			Root: root,
			ByID: map[string]*FlowContractView{
				"child": &root.Children[0],
			},
		},
		flowTypes: map[string]TypeCatalogDocument{
			"child": {
				Types: map[string]NamedTypeDecl{
					"Evidence": {
						Fields: map[string]TypeFieldSpec{
							"child_field": {Type: "text"},
						},
					},
				},
			},
		},
	}

	for _, eventType := range []string{"handoff.completed", "child/handoff.completed"} {
		t.Run(eventType, func(t *testing.T) {
			schema, key, ok := EventSchemaForFlowEvent(bundle, "child", eventType)
			if !ok {
				t.Fatal("missing child output event schema")
			}
			if key != "handoff.completed" {
				t.Fatalf("schema key = %q, want root declaration handoff.completed", key)
			}
			props, _ := schema.Schema["properties"].(map[string]any)
			evidence, _ := props["evidence"].(map[string]any)
			evidenceProps, _ := evidence["properties"].(map[string]any)
			if _, ok := evidenceProps["root_field"]; !ok {
				t.Fatalf("evidence properties = %#v, want root_field", evidenceProps)
			}
			if _, ok := evidenceProps["child_field"]; ok {
				t.Fatalf("evidence properties = %#v, must not use child Evidence override for root-declared event", evidenceProps)
			}
		})
	}
	if _, _, ok := EventSchemaForFlowEvent(bundle, "child", "sibling/handoff.completed"); ok {
		t.Fatal("sibling-qualified event must not match root declaration by leaf name")
	}
}

func currentPlatformSpecForSchemaRegistryTest(t *testing.T) PlatformSpecDocument {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
	var spec PlatformSpecDocument
	if err := loadYAMLFile(DefaultPlatformSpecFile(dir), &spec); err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	return spec
}
