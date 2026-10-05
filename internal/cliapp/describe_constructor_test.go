package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestDescribeRendersCanonicalConstructors(t *testing.T) {
	root := canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Mode: canonicalrouting.TemplateInstanceRouteCreate})
	writeDescribeTestFile(t, filepath.Join(root, "producer/events.yaml"), "deploy.done:\n  vertical_id: text\n  brief: text\n")
	writeDescribeTestFile(t, filepath.Join(root, "consumer/entities.yaml"), "deployment:\n  vertical_id: text\n  brief: text\n")
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), root, contracts.DefaultPlatformSpecFile(RepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	constructors, err := pipeline.CompileFlowConstructors(semanticview.Wrap(bundle), "consumer")
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root, "--json"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe: %d %s", code, stderr.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	var child []authoringview.FlowConstructorView
	for _, flow := range view.Flows {
		if flow.ID == "consumer" {
			child = flow.Constructors
		}
	}
	if len(child) != 1 || len(constructors) != 1 || child[0].Input != constructors[0].Input() || child[0].KeyField != constructors[0].KeyField() || !reflect.DeepEqual(child[0].SuppliedFields, constructors[0].SuppliedFields()) {
		t.Fatalf("describe differs from canonical constructor: %+v", child)
	}
	if len(view.Root.Constructors) != 1 || !view.Root.Constructors[0].NoArguments || len(view.Root.Constructors[0].SuppliedFields) != 0 {
		t.Fatalf("root constructor: %+v", view.Root.Constructors)
	}
	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe: %d %s", code, stderr.String())
	}
	for _, want := range []string{"root constructor: (no arguments)", "constructor: deploy.done key=vertical_id supplied=[brief]"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("missing %q: %s", want, stdout.String())
		}
	}
}
