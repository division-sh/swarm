package contracts

import "testing"

func TestEventSchemaRegistryFromCatalog_NormalizesAnnotatedFieldTypes(t *testing.T) {
	registry := EventSchemaRegistryFromCatalog(map[string]EventCatalogEntry{
		"scan.requested": {
			Payload: EventPayloadSpec{
				Properties: map[string]EventFieldSpec{
					"corpus_path":   {Type: "string (required for corpus mode — path to signals file in /data)"},
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
	score, _ := props["score"].(map[string]any)
	if score["type"] != "number" {
		t.Fatalf("score type = %#v", score["type"])
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
