package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestLifecycleEmitterDescribeUsesCanonicalTopology(t *testing.T) {
	for _, tc := range []struct {
		name string
		root func(*testing.T) string
	}{
		{"gate", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateLocal)
		}},
		{"loop_scopes", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticLoopScopeMatrix)
		}},
		{"loop_wildcards", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterLoopScopeTopology(t, canonicalrouting.LifecycleLoopScopeWildcard)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.root(t)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			report := bootverify.Run(context.Background(), source, bootverify.Options{})
			want := authoringview.BuildRoutingTopologyWithReport(source, bundle, &report)
			canonical := routingtopology.Build(source)
			if !reflect.DeepEqual(want.Producers, canonical.Producers) || !reflect.DeepEqual(want.Edges, canonical.Edges) || !reflect.DeepEqual(want.BoundaryExposures, canonical.BoundaryExposures) {
				t.Fatal("authoring diagnostics changed canonical routing facts")
			}
			run := func(args ...string) []byte {
				t.Helper()
				var stdout, stderr bytes.Buffer
				if code := executeRootCommandWithOptions(context.Background(), repo, args, &stdout, &stderr, defaultRootCommandOptions()); code != 0 || stderr.Len() != 0 {
					t.Fatalf("%v: code=%d stderr=%s stdout=%s", args, code, stderr.String(), stdout.String())
				}
				return stdout.Bytes()
			}
			wire := run("describe", "routes", root, "--json")
			var routes routingtopology.Topology
			if err := json.Unmarshal(wire, &routes); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(routes, want) {
				t.Fatalf("CLI routes disagree with tested projection: %s", wire)
			}
			var full describeCommandOutput
			if err := json.Unmarshal(run("describe", root, "--json"), &full); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(full.RoutingTopology, routes) {
				t.Fatal("full describe changed lifecycle topology or provenance")
			}
			if again := run("describe", "routes", root, "--json"); !bytes.Equal(again, wire) {
				t.Fatal("repeated describe changed serialized topology")
			}
		})
	}
}
