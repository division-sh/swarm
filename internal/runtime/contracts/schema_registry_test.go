package contracts

import "testing"

func TestEventSchemaRegistryFromCatalog_NormalizesAnnotatedFieldTypes(t *testing.T) {
	registry := EventSchemaRegistryFromCatalog(map[string]EventCatalogEntry{
		"scan.requested": {
			Payload: EventPayloadSpec{
				Properties: map[string]EventFieldSpec{
					"corpus_path":   {Type: "string (required for corpus mode — path to signals file in /data)"},
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
	subcategories, _ := props["subcategories"].(map[string]any)
	if subcategories["type"] != "array" {
		t.Fatalf("subcategories type = %#v", subcategories["type"])
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
