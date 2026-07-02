package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templateflowpilot"
)

func TestDescribeCommandJSONRendersExpandedAuthoringView(t *testing.T) {
	contractsRoot := templateflowpilot.Write(t, templateflowpilot.Options{})
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), repoRoot(), []string{
		"describe",
		"--contracts", contractsRoot,
		"--json",
	}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe --json code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("describe --json stderr = %q, want empty", stderr.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); err != nil {
		t.Fatalf("decode describe json: %v\n%s", err, stdout.String())
	}
	if view.SourceAuthority != "projection_only_existing_contract_owners" {
		t.Fatalf("source authority = %q, want projection marker", view.SourceAuthority)
	}
	if len(view.ConnectRoutePlans) != 1 {
		t.Fatalf("connect route plans = %#v, want one", view.ConnectRoutePlans)
	}
	plan := view.ConnectRoutePlans[0]
	if plan.ResolutionKind != "instance_key" || plan.InstanceKey == nil {
		t.Fatalf("route plan = %#v, want instance_key plan", plan)
	}
	if plan.Source.Key != "account_id" || len(plan.Source.Carries) == 0 {
		t.Fatalf("route source = %#v, want output key/carries", plan.Source)
	}
	if len(view.Flows) != 2 {
		t.Fatalf("flow count = %d, want 2", len(view.Flows))
	}
}
