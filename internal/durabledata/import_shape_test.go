package durabledata

import (
	"strings"
	"testing"
)

func TestImportShapeValidatesBoundsIdentityAndTextKey(t *testing.T) {
	shape := ImportShape{
		BundleHash:   "bundle-v2:sha256:" + strings.Repeat("a", 64),
		Declaration:  DeclarationRef{FlowPath: ".", EventName: "candidate.created"},
		SchemaDigest: SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64)),
		BusinessKey:  "id",
		Fields: []ImportShapeField{
			{Name: "id", Required: true, Text: true},
			{Name: "resume", Required: true, Text: true},
			{Name: "score", Required: false},
		},
	}
	if err := shape.Validate(); err != nil || shape.TextBusinessKey() != "id" {
		t.Fatalf("valid text-key shape = %q, %v", shape.TextBusinessKey(), err)
	}
	shape.Fields[0].Text = false
	if err := shape.Validate(); err != nil || shape.TextBusinessKey() != "" {
		t.Fatalf("numeric key shape = %q, %v", shape.TextBusinessKey(), err)
	}
	shape.Fields[0].Required = false
	if err := shape.Validate(); err == nil {
		t.Fatal("optional business key was accepted")
	}
	shape.Fields[0].Required = true
	shape.Fields[1].Name = "id"
	if err := shape.Validate(); err == nil {
		t.Fatal("duplicate field was accepted")
	}
	shape.Fields[1].Name = "resume"
	shape.BundleHash = "wrong"
	if err := shape.Validate(); err == nil {
		t.Fatal("wrong bundle hash was accepted")
	}
}
