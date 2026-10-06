package bootverify

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestScalar2556GateContextSourceDiagnostic(t *testing.T) {
	for _, value := range []string{"ready", "${entity.name}", "'ready'", `"${7}"`} {
		t.Run(value, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: diagnostic\nstages:\n  waiting:\n    gate:\n      decision: review\n      context:\n        note: "+value+"\n      outcomes:\n        accept: {advances_to: done}\n  done: {final: true}\n")
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
			findings := checkStageGateValidation(&checkerContext{source: semanticview.Wrap(bundle)})
			if strings.HasPrefix(value, "'") || strings.HasPrefix(value, `"`) {
				if len(findings) != 0 {
					t.Fatalf("quoted text rejected: %#v", findings)
				}
				return
			}
			if len(findings) != 1 {
				t.Fatalf("expected one expression error, got %#v", findings)
			}
			for _, want := range []string{"expression slot stages.waiting.gate.context.note", "schema.yaml:7:"} {
				if !strings.Contains(findings[0].Message, want) {
					t.Fatalf("missing %q: %s", want, findings[0].Message)
				}
			}
			if strings.Contains(findings[0].Message, "quote text or use a declared reference") != (value == "ready") {
				t.Fatalf("undeclared-root teaching changed parse diagnostics: %s", findings[0].Message)
			}
		})
	}
}
