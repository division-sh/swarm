package bootverify

import (
	"context"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"strings"
	"testing"
)

func TestLeadReview6BootDerivedCollectionLookup(t *testing.T) {
	for _, expr := range []string{`(true ? payload.items : payload.items)[0].status == "active"`, `([] + payload.items)[0].status == "active"`} {
		t.Run(expr, func(t *testing.T) {
			source := collectionItemSemanticsSource(rc.SystemNodeEventHandler{Guard: &rc.GuardSpec{Check: expr}})
			bundle, _ := semanticview.Bundle(source)
			bundle.Platform.Platform.Name = "review"
			bundle.Platform.Platform.Version = "1"
			entity := bundle.RootEntities["items"]
			for name, field := range entity.Fields {
				field.UnusedReason = "externally populated review fixture"
				entity.Fields[name] = field
			}
			bundle.RootEntities["items"] = entity
			report := Run(context.Background(), source, Options{})
			found := false
			for _, f := range report.Errors() {
				t.Logf("%s: %s", f.CheckID, f.Message)
				if strings.Contains(f.Message, "lookup") {
					found = true
				}
			}
			if !found {
				t.Fatalf("boot admitted unsafe derived collection lookup; total errors=%d", len(report.Errors()))
			}
		})
	}
}
