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
