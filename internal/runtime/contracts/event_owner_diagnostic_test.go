package contracts

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestEventOwnerDiagnosticFailingSourceLoads(t *testing.T) {
	for _, tc := range []struct {
		name, nodes string
		owners      []string
	}{
		{
			name:  "direct",
			nodes: "alpha:\n  event_handlers:\n    work.requested:\n      advances_to: done\nbeta:\n  event_handlers:\n    work.requested:\n      advances_to: done\n",
			owners: []string{
				`node "alpha" (scope "."): handler "work.requested" at nodes.yaml:4:7`,
				`node "beta" (scope "."): handler "work.requested" at nodes.yaml:8:7`,
			},
		},
		{
			name:  "alias",
			nodes: "first: &node\n  event_handlers:\n    work.requested:\n      advances_to: done\nsecond: *node\n",
			owners: []string{
				`node "first" (scope "."): handler "work.requested" at nodes.yaml:4:7`,
				`node "second" (scope "."): handler "work.requested" at nodes.yaml:5:9`,
			},
		},
		{
			name:  "merge",
			nodes: "first: &node\n  event_handlers:\n    work.requested:\n      advances_to: done\nsecond:\n  <<: *node\n",
			owners: []string{
				`node "first" (scope "."): handler "work.requested" at nodes.yaml:4:7`,
				`node "second" (scope "."): handler "work.requested" at nodes.yaml:6:7`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: owner-diagnostic\nstages: {waiting: {}, done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested:\n")
			writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), tc.nodes)
			assertEventOwnerDiagnosticLoads(t, root, "work.requested", tc.owners)
		})
	}
}

func TestEventOwnerDiagnosticJoinUntilUsesContributingDeclaration(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: join-owner-diagnostic\nstages: {waiting: {}, done: {final: true}}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "arrived:\n  member: text\n  result: integer\nclosed:\nunrelated:\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), `collector:
  event_handlers:
    unrelated: {advances_to: done}
    arrived:
      join:
        stage: waiting
        members: {count: 0, by: payload.member}
        output: payload.result
        until: closed
        on_complete: {advances_to: done}
closer:
  event_handlers:
    closed:
      advances_to: done
`)
	assertEventOwnerDiagnosticLoads(t, root, "closed", []string{
		`node "closer" (scope "."): handler "closed" at nodes.yaml:14:7`,
		`node "collector" (scope "."): join.until in handler "arrived" at nodes.yaml:9:16`,
	})
}

func TestEventOwnerDiagnosticScopedAndPatternCoordinates(t *testing.T) {
	for _, pattern := range []string{"work.requested", "work.*"} {
		t.Run(pattern, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-owner-diagnostic\n")
			writeFixtureFile(t, filepath.Join(root, "parent", "jobs", "schema.yaml"), "name: jobs\nstages: {waiting: {}, done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, "parent", "jobs", "events.yaml"), "work.requested:\n")
			writeFixtureFile(t, filepath.Join(root, "parent", "jobs", "nodes.yaml"), "alpha:\n  event_handlers:\n    "+pattern+":\n      advances_to: done\nbeta:\n  event_handlers:\n    "+pattern+":\n      advances_to: done\n")
			assertEventOwnerDiagnosticLoads(t, root, "parent/jobs/"+pattern, []string{
				`node "alpha" (scope "parent/jobs"): handler "` + pattern + `" at parent/jobs/nodes.yaml:4:7`,
				`node "beta" (scope "parent/jobs"): handler "` + pattern + `" at parent/jobs/nodes.yaml:8:7`,
			})
		})
	}
}

func TestEventOwnerDiagnosticSameSlugDistinctScopes(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: distinct-owner-diagnostic\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested:\n")
	for _, scope := range []string{"west", "east"} {
		writeFixtureFile(t, filepath.Join(root, scope, "schema.yaml"), "name: "+scope+"\nstages: {waiting: {}, done: {final: true}}\n")
		writeFixtureFile(t, filepath.Join(root, scope, "nodes.yaml"), "worker:\n  event_handlers:\n    work.requested:\n      advances_to: done\n")
	}
	assertEventOwnerDiagnosticLoads(t, root, "work.requested", []string{
		`node "worker" (scope "east"): handler "work.requested" at east/nodes.yaml:4:7`,
		`node "worker" (scope "west"): handler "work.requested" at west/nodes.yaml:4:7`,
	})
}

func assertEventOwnerDiagnosticLoads(t *testing.T, root, event string, owners []string) {
	t.Helper()
	repo := repoRootForContractsTest(t)
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	var first string
	for _, loader := range []struct {
		name string
		load func() (*WorkflowContractBundle, error)
	}{
		{"disk", func() (*WorkflowContractBundle, error) {
			return LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
		}},
		{"artifact", func() (*WorkflowContractBundle, error) {
			return LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		}},
		{"retained", func() (*WorkflowContractBundle, error) {
			return LoadWorkflowContractBundleFromArtifact(repo, retained, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		}},
	} {
		t.Run(loader.name, func(t *testing.T) {
			bundle, err := loader.load()
			if bundle != nil || !errors.Is(err, ErrMultipleAuthoritativeOwners) {
				t.Fatalf("expected typed refusal before the final provenance ledger exists: bundle=%v error=%v", bundle, err)
			}
			message := err.Error()
			for _, want := range append([]string{
				"event " + event + " has multiple authoritative system node owners",
				"one owning system node",
				"one router node",
				"ordered rules:",
			}, owners...) {
				if !strings.Contains(message, want) {
					t.Errorf("missing %q in %s", want, message)
				}
			}
			if strings.Contains(message, identitytest.RootNode(t, "alpha").Key()) || strings.Contains(message, identitytest.RootNode(t, "beta").Key()) {
				t.Errorf("encoded node identity leaked: %s", message)
			}
			if first == "" {
				first = message
			} else if message != first {
				t.Errorf("source loaders disagree: first=%s got=%s", first, message)
			}
			for i := 1; i < len(owners); i++ {
				if strings.Index(message, owners[i-1]) >= strings.Index(message, owners[i]) {
					t.Errorf("owners not deterministically ordered: %s", message)
				}
			}
		})
	}
}

func TestEventOwnerDiagnosticUsesCompiledCoordinatesOnly(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: retained-owner-diagnostic\n")
	writeFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\n")
	writeFixtureFile(t, filepath.Join(root, "child", "events.yaml"), "work.requested:\nwork.done:\n")
	writeFixtureFile(t, filepath.Join(root, "child", "nodes.yaml"), "worker:\n  event_handlers:\n    work.requested: {emit: work.done}\n")
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	node := identitytest.ExecutableNode(t, "child", "worker")
	want := multipleEventOwnersError(bundle, "child/work.requested", []string{node.Key()}).Error()
	for _, transition := range bundle.Semantics.HandlerTransitions {
		if transition.Node.Equal(node) && transition.ownerEventType != "child/work.requested" {
			t.Fatalf("compiled handler lost the canonical ownership coordinate: %+v", transition)
		}
	}
	// Changing raw event catalogs must not re-resolve the ownership coordinate.
	bundle.FlowTree.ByID["child"].Events = nil
	if resolved := bundle.ResolveExecutableNodeEventPattern(node, "work.requested"); resolved == "child/work.requested" {
		t.Fatal("hostile catalog change did not distinguish retained from reconstructed coordinates")
	}
	if got := multipleEventOwnersError(bundle, "child/work.requested", []string{node.Key()}).Error(); got != want {
		t.Fatalf("diagnostic reconstructed ownership from raw catalogs: %s != %s", got, want)
	}
	if got := multipleEventOwnersError(bundle, "child/work.done", []string{node.Key()}).Error(); strings.Contains(got, `handler "work.requested"`) {
		t.Fatalf("unrelated handler acquired a diagnostic contribution: %s", got)
	}
	bundle.Semantics.HandlerTransitions = nil
	if got := multipleEventOwnersError(bundle, "child/work.requested", []string{node.Key()}).Error(); !strings.Contains(got, `node "worker" (scope "child"): location unavailable`) {
		t.Fatalf("missing compiled declaration must not be reconstructed: %s", got)
	}
}

func TestEventOwnerDiagnosticMissingSourceAndStableOwnerSet(t *testing.T) {
	alpha, beta := identitytest.RootNode(t, "alpha"), identitytest.RootNode(t, "beta")
	var first string
	for _, owners := range [][]string{
		{beta.Key(), alpha.Key(), beta.Key(), " "},
		{alpha.Key(), beta.Key()},
	} {
		bundle := &WorkflowContractBundle{Semantics: WorkflowSemanticView{EventOwners: map[string][]string{"work.requested": owners}}}
		err := validateWorkflowContractBundleLoadConstraints(bundle)
		if !errors.Is(err, ErrMultipleAuthoritativeOwners) {
			t.Fatalf("missing source lost the typed refusal: %v", err)
		}
		message := err.Error()
		for _, want := range []string{
			`node "alpha" (scope "."): location unavailable`,
			`node "beta" (scope "."): location unavailable`,
		} {
			if !strings.Contains(message, want) {
				t.Errorf("missing owner/location fallback %q: %s", want, message)
			}
		}
		if first == "" {
			first = message
		} else if first != message {
			t.Fatalf("normalization changed the displayed owner set/order: %s != %s", first, message)
		}
	}
	bundle := &WorkflowContractBundle{Semantics: WorkflowSemanticView{EventOwners: map[string][]string{"work.requested": {alpha.Key(), alpha.Key(), " "}}}}
	if err := validateWorkflowContractBundleLoadConstraints(bundle); err != nil {
		t.Fatalf("one normalized owner was refused: %v", err)
	}
}

func TestEventOwnerDiagnosticCorrectedRouterLoads(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: owner-router\nstages: {waiting: {}, done: {final: true}}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested:\n  priority: integer\nwork.urgent:\nwork.normal:\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), `router:
  event_handlers:
    work.requested:
      rules:
        - when: payload.priority > 0
          emit: work.urgent
        - emit: work.normal
urgent:
  event_handlers:
    work.urgent: {advances_to: done}
normal:
  event_handlers:
    work.normal: {advances_to: done}
`)
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	for event, want := range map[string]string{"work.requested": "router", "work.urgent": "urgent", "work.normal": "normal"} {
		owners := bundle.Semantics.EventOwners[event]
		if len(owners) != 1 || owners[0] != identitytest.RootNode(t, want).Key() {
			t.Fatalf("router correction changed event ownership: %s=%v", event, owners)
		}
	}
}
