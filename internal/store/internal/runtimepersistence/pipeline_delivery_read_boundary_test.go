package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPipelineDeliveryReadClassificationRejectsForeignOperations(t *testing.T) {
	path := "internal/store/internal/backend/pipelinepersistence/deployment_observation.go"
	bytes, err := os.ReadFile(filepath.Join(eventBoundaryRepositoryRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	source := string(bytes)
	for _, tc := range []struct {
		name, path, source string
		wantUnclassified   bool
	}{
		{"exact_four", path, source, false},
		{"foreign_recipient", path, strings.Replace(source, "r.event_id=e.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'", "r.event_id=e.event_id AND r.subscriber_type='agent' AND r.subscriber_id='foreign'", 1), true},
		{"extra_query", path, strings.Replace(source, "var out DeploymentEventDeliveryCutStorage", "_, _ = tx.ExecContext(ctx, `DELETE FROM event_deliveries`)\nvar out DeploymentEventDeliveryCutStorage", 1), true},
		{"other_declaration", path, source + "\nconst escape = `SELECT * FROM event_deliveries`\n", true},
		{"other_file", "internal/store/internal/backend/pipelinepersistence/foreign.go", source, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), tc.path, tc.source, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			classified := classifiedPipelineDeliveryReads(tc.path, file)
			unclassified := 0
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				query, err := strconv.Unquote(literal.Value)
				if err == nil && executableDeliverySQL.MatchString(query) && !classified[literal] {
					unclassified++
				}
				return true
			})
			if (unclassified > 0) != tc.wantUnclassified {
				t.Fatalf("exact read-only ownership classification changed: unclassified=%d want violation=%t", unclassified, tc.wantUnclassified)
			}
		})
	}
}
