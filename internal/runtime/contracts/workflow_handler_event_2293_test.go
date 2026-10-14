package contracts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestNodeHandlerEventAdmissionDirectoryAndRetainedSource(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, tc := range []struct {
		name, event, declared, want string
	}{
		{name: "exact empty consumer", event: "work.requested", declared: "work.requested"},
		{name: "dotless", event: "request", declared: "request"},
		{name: "wildcard concrete", event: "work.*", declared: "work.requested"},
		{name: "wildcard suffix", event: "*.requested", declared: "work.requested"},
		{name: "wildcard all", event: "*", declared: "work.requested"},
		{name: "declared wildcard", event: "work.*", declared: "work.*"},
		{name: "undeclared", event: "work.unknown", declared: "work.requested", want: "undeclared event"},
		{name: "misspelled node field", event: "state_schma", declared: "work.requested", want: "undeclared event"},
		{name: "unmatched wildcard", event: "typo.*", declared: "work.requested", want: "undeclared event"},
		{name: "unregistered platform", event: "platform.not_declared_2293", declared: "work.requested", want: "undeclared event"},
		{name: "unregistered timer", event: "timer.not_declared_2293", declared: "work.requested", want: "undeclared event"},
		{name: "unsupported wildcard", event: "work.pre*", declared: "work.requested", want: "exact event name or supported wildcard pattern"},
		{name: "reserved timers", event: "timers", declared: "timers", want: "is a node field, not an event"},
		{name: "reserved state schema", event: "state_schema", declared: "state_schema", want: "is a node field, not an event"},
		{name: "reserved gate state", event: "gate_state", declared: "gate_state", want: "is a node field, not an event"},
		{name: "reserved state table", event: "state_table", declared: "state_table", want: "is a node field, not an event"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: handler-admission\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), fmt.Sprintf("%q:\n", tc.declared))
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), fmt.Sprintf("worker:\n  event_handlers:\n    %q: {}\n", tc.event))
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for _, ingress := range []struct {
				name string
				load func() (*WorkflowContractBundle, error)
			}{
				{"directory", func() (*WorkflowContractBundle, error) {
					return LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				}},
				{"retained", func() (*WorkflowContractBundle, error) {
					return LoadWorkflowContractBundleFromArtifact(repo, retained, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
				}},
			} {
				t.Run(ingress.name, func(t *testing.T) {
					bundle, err := ingress.load()
					if tc.want != "" {
						if bundle != nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), tc.event) || !strings.Contains(err.Error(), "nodes.yaml:3:") {
							t.Fatalf("exact handler declaration refusal: bundle=%v error=%v, want %q for %q at nodes.yaml:3", bundle != nil, err, tc.want, tc.event)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if handler, ok := bundle.Nodes["worker"].EventHandlers[tc.event]; !ok || len(HandlerEmitEvents(handler, nil)) != 0 {
						t.Fatalf("explicit empty handler did not remain a no-op consumer: %#v", handler)
					}
				})
			}
		})
	}
}

func TestNodeHandlerEventAdmissionKeepsGeneratedAndPlatformDeclarations(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, flow := range []string{".", "child"} {
		for _, event := range []string{"lookup.succeeded", "lookup.failed", "lookup.*", "*.succeeded", "platform.boot", "platform.*", "other.succeeded", "other.*"} {
			t.Run(flow+"/"+event, func(t *testing.T) {
				root := t.TempDir()
				writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: generated-handler-admission\n")
				dir := filepath.Join(root, flow)
				if flow != "." {
					writeFixtureFile(t, filepath.Join(dir, "schema.yaml"), "name: child\n")
				}
				writeFixtureFile(t, filepath.Join(dir, "events.yaml"), "source.requested:\n")
				writeFixtureFile(t, filepath.Join(dir, "tools.yaml"), `lookup:
  description: Read a source.
  handler_type: http
  effect_class: read_only
  http: {method: GET, url: https://example.test}
`)
				writeFixtureFile(t, filepath.Join(dir, "nodes.yaml"), fmt.Sprintf(`worker:
  event_handlers:
    source.requested:
      activity: {id: lookup, tool: lookup}
    %q: {}
`, event))
				bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				if strings.HasPrefix(event, "other.") {
					if bundle != nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "undeclared event") || !strings.Contains(err.Error(), event) {
						t.Fatalf("nonexistent generated event admitted: bundle=%v error=%v", bundle != nil, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				generated, ok, err := bundle.ResolveCompiledFlowEventSchema(flow, "lookup.succeeded")
				if err != nil || !ok || generated.Classification() != CompiledEventSchemaGenerated {
					t.Fatalf("activity result lost generated declaration ownership: classification=%q present=%v error=%v", generated.Classification(), ok, err)
				}
			})
		}
	}
}

func TestNodeHandlerEventAdmissionPreservesIntroductionCoordinates(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, tc := range []struct {
		name, source string
		line, column int
	}{
		{"alias", "first: &node\n  event_handlers:\n    typo.ready: {}\nsecond: *node\n", 4, 9},
		{"merge", "first: &node\n  event_handlers:\n    typo.ready: {}\nsecond:\n  <<: *node\n", 5, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: coordinate-admission\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), "task.ready:\n")
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), tc.source)
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
			if err != nil {
				t.Fatal(err)
			}
			for _, ingress := range []string{"directory", "retained"} {
				t.Run(ingress, func(t *testing.T) {
					var bundle *WorkflowContractBundle
					var err error
					if ingress == "directory" {
						bundle, err = LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
					} else {
						bundle, err = LoadWorkflowContractBundleFromArtifact(repo, retained, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
					}
					want := fmt.Sprintf("node %s handler %q at ", identitytest.RootNode(t, "second").Key(), "typo.ready")
					coordinate := fmt.Sprintf("nodes.yaml:%d:%d refers to an undeclared event", tc.line, tc.column)
					if bundle != nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), coordinate) {
						t.Fatalf("handler admission lost introduction coordinate: bundle=%v error=%v, want %q and %q", bundle != nil, err, want, coordinate)
					}
				})
			}
		})
	}
}

func TestNodeHandlerEventAdmissionKeepsConnectedReceiverOwnership(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, event := range []string{"work.ready", "work.*"} {
		t.Run(event, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
			writeFixtureFile(t, filepath.Join(root, "consumer", "nodes.yaml"), fmt.Sprintf("consumer-node:\n  event_handlers:\n    %q: {}\n", event))
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			owner := identitytest.FlowNode(t, "consumer", "consumer-node")
			_, key, ok := bundle.ResolveExecutableNodeEventCatalogEntry(owner, "work.ready")
			if !ok || key != "producer/work.ready" {
				t.Fatalf("receiver declaration borrowed or restated producer ownership: key=%q ok=%v", key, ok)
			}
		})
	}
}

func TestNodeHandlerEventAdmissionRejectsSiblingDeclarationBorrow(t *testing.T) {
	repo := contractRepoRoot(t)
	for _, event := range []string{"foreign.ready", "foreign.*", "producer/foreign.ready"} {
		t.Run(event, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
			producerEvents := filepath.Join(root, "producer", "events.yaml")
			events, err := os.ReadFile(producerEvents)
			if err != nil {
				t.Fatal(err)
			}
			writeFixtureFile(t, producerEvents, string(events)+"\nforeign.ready:\n")
			writeFixtureFile(t, filepath.Join(root, "consumer", "nodes.yaml"), fmt.Sprintf("consumer-node:\n  event_handlers:\n    %q: {}\n", event))
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if bundle != nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "undeclared event") || !strings.Contains(err.Error(), event) {
				t.Fatalf("sibling declaration granted handler admission: bundle=%v error=%v", bundle != nil, err)
			}
		})
	}
}
