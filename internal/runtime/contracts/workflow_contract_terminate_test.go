package contracts

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestAuthoredTerminateRequiresAnActualTransition(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
		terminate  bool
	}{
		{"absent", "advances_to: done\n", true, false},
		{"explicit", "advances_to: done\nterminate: true\n", true, true},
		{"false", "advances_to: done\nterminate: false\n", true, false},
		{"missing_target", "terminate: true\n", false, false},
		{"empty_target", "advances_to: ''\nterminate: true\n", false, false},
		{"quoted", "advances_to: done\nterminate: 'true'\n", false, false},
		{"null", "advances_to: done\nterminate: null\n", false, false},
		{"mapping", "advances_to: done\nterminate: {}\n", false, false},
		{"sequence", "advances_to: done\nterminate: []\n", false, false},
	} {
		for _, carrier := range []string{"handler", "rule", "on_complete", "join.on_complete"} {
			t.Run(carrier+"/"+tc.name, func(t *testing.T) {
				var handler SystemNodeEventHandler
				body := tc.body
				if carrier != "handler" {
					body = "id: selected\n" + body
					switch carrier {
					case "rule":
						body = "rules:\n  - else: true\n" + indentTerminateFixture(body, "    ")
					case "on_complete":
						body = "on_complete:\n  - condition: true\n" + indentTerminateFixture(body, "    ")
					case "join.on_complete":
						body = "join:\n  stage: awaiting\n  members: {from: state.ids, by: payload.id}\n  output: payload.result\n  on_complete:\n" + indentTerminateFixture(tc.body, "    ")
					}
				}
				err := decodeNodeTestYAML([]byte(body), &handler)
				if (err == nil) != tc.valid {
					t.Fatalf("valid=%t want=%t error=%v", err == nil, tc.valid, err)
				}
				if err != nil {
					if !strings.Contains(err.Error(), "terminate") {
						t.Fatalf("failure did not name terminate: %v", err)
					}
					return
				}
				var got bool
				switch carrier {
				case "handler":
					got = handler.Terminate
				case "rule":
					got = handler.Rules[0].Terminate
				case "on_complete":
					got = handler.OnComplete[0].Terminate
				case "join.on_complete":
					got = handler.Join.OnComplete.Terminate
				}
				if got != tc.terminate {
					t.Fatalf("authored terminate=%t want=%t", got, tc.terminate)
				}
			})
		}
	}
}

func TestTerminateDiskAndReconstructedSourceAgree(t *testing.T) {
	for _, raw := range []string{"true", "false", "null", "''", "'true'", "[]", "{}"} {
		t.Run(raw, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "nodes.yaml")
			body := "worker:\n  event_handlers:\n    work.finished:\n      advances_to: done\n      terminate: " + raw + "\n"
			writeFixtureFile(t, path, body)
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
			valid := raw == "true" || raw == "false"
			if (diskErr == nil) != valid || (retainedErr == nil) != valid {
				t.Fatalf("terminate admission differs: disk=%v retained=%v want valid=%t", diskErr, retainedErr, valid)
			}
			if valid && (!reflect.DeepEqual(disk["worker"].EventHandlers, retained["worker"].EventHandlers) || disk["worker"].EventHandlers["work.finished"].Terminate != (raw == "true")) {
				t.Fatal("reconstructed source changed the authored cancellation intent")
			}
		})
	}
}

func indentTerminateFixture(body, indent string) string {
	return indent + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n"+indent) + "\n"
}
