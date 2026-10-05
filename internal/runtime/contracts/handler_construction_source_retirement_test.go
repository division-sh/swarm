package contracts

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestHandlerConstructionRetirementDiskAndReconstructedSource(t *testing.T) {
	for _, variant := range []string{"absent", "", "true", "false", "null", "''", "[]", "{}", "[true]", "{value: true}"} {
		t.Run(variant, func(t *testing.T) {
			body := "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested:\n"
			if variant != "absent" {
				body += "      create_entity: " + variant + "\n"
			}
			body += "      emit: work.completed\n"
			root := t.TempDir()
			path := filepath.Join(root, "nodes.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			disk, diskErr := loadOptionalNodeDeclarations(path)
			retained, retainedErr := loadOptionalNodeDeclarationsFromSource(restored, "nodes.yaml")
			if variant == "absent" {
				if diskErr != nil || retainedErr != nil || len(disk) != 1 || len(retained) != 1 || !reflect.DeepEqual(disk["worker"].EventHandlers, retained["worker"].EventHandlers) || disk["worker"].EventHandlers["work.requested"].CreateEntity {
					t.Fatalf("ordinary handler parity: disk=%+v retained=%+v errors=%v/%v", disk, retained, diskErr, retainedErr)
				}
				return
			}
			for _, err := range []error{diskErr, retainedErr} {
				if err == nil || !strings.Contains(err.Error(), `handler field "create_entity" is not supported`) || !strings.Contains(err.Error(), "Valid fields:") {
					t.Fatalf("retired presence %q did not fail closed with constructor teaching: %v", variant, err)
				}
			}
		})
	}
}
