package bootverify

import (
	"context"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"strings"
	"testing"
)

func TestLeadReviewEntityNestedOptional(t *testing.T) {
	for _, expr := range []string{`entity.profile.note != ""`, `has(entity.profile.note) && entity.profile.note != ""`, `entity.profile.?note.orValue("") != ""`} {
		t.Run(expr, func(t *testing.T) {
			source := collectionItemSemanticsSource(rc.SystemNodeEventHandler{Guard: &rc.GuardSpec{Check: expr}})
			bundle, _ := semanticview.Bundle(source)
			bundle.Platform.Platform.Name = "review"
			bundle.Platform.Platform.Version = "1"
			entity := bundle.RootEntities["items"]
			entity.Fields["profile"] = rc.EntityFieldDecl{Type: "WorkItem", Initial: map[string]any{"id": "review", "status": "new", "tags": []any{}}}
			for name, field := range entity.Fields {
				field.UnusedReason = "externally populated review fixture"
				entity.Fields[name] = field
			}
			bundle.RootEntities["items"] = entity
			report := Run(context.Background(), source, Options{})
			found := false
			for _, f := range report.Errors() {
				t.Logf("%s: %s", f.CheckID, f.Message)
				if strings.Contains(f.Message, "note") {
					found = true
				}
			}
			if expr == `entity.profile.note != ""` && !found {
				t.Errorf("boot admitted undecided optional named-record field under entity; allErrors=%d", len(report.Errors()))
			}
			if expr == `entity.profile.?note.orValue("") != ""` && len(report.Errors()) > 0 {
				t.Error("stock optional decision rejected by field-path interpreter")
			}
		})
	}
}
