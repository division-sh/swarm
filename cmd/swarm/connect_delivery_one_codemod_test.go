package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestMigrateConnectDeliveryOneCommandRewritesLoadableEquivalentEdge(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	canonicalrouting.ApplyRetiredConnectDeliveryOneMutation(t, root)

	var out bytes.Buffer
	var errOut bytes.Buffer
	code := executeRootCommand(context.Background(), repoRoot, []string{
		"migrate-connect-delivery-one",
		"--contracts", root,
	}, &out, &errOut)
	if code != 0 {
		t.Fatalf("migrate command code = %d, stderr = %q", code, errOut.String())
	}
	if !strings.Contains(out.String(), "removed 1 retired connect.delivery: one declaration") {
		t.Fatalf("stdout = %q, want deterministic removal report", out.String())
	}

	raw, err := os.ReadFile(filepath.Join(root, "package.yaml"))
	if err != nil {
		t.Fatalf("read rewritten package.yaml: %v", err)
	}
	if strings.Contains(string(raw), "delivery:") {
		t.Fatalf("rewritten package.yaml retains delivery:\n%s", raw)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load rewritten bundle: %v", err)
	}
	connects := semanticview.Wrap(bundle).CompositionConnects()
	if len(connects) != 1 || connects[0].From != "producer.work_ready" || connects[0].To != "consumer.work_ready" {
		t.Fatalf("rewritten connects = %#v, want unchanged producer.work_ready -> consumer.work_ready edge", connects)
	}
	plans, issues := pinrouting.LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 || len(plans) != 1 {
		t.Fatalf("lower rewritten bundle plans/issues = %#v/%#v, want one valid edge plan", plans, issues)
	}
	if plans[0].Source.FlowID != "producer" || plans[0].Receiver.FlowID != "consumer" || plans[0].Target.FlowID != "consumer" {
		t.Fatalf("rewritten route plan = %#v, want unchanged producer-to-consumer semantics", plans[0])
	}

	out.Reset()
	errOut.Reset()
	code = executeRootCommand(context.Background(), repoRoot, []string{
		"migrate-connect-delivery-one",
		"--contracts", root,
	}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "no connect.delivery: one declarations found") {
		t.Fatalf("idempotent migrate code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
	}
}

func TestMigrateConnectDeliveryOneCommandLeavesManualCasesUntouched(t *testing.T) {
	tests := []struct {
		name    string
		blocker canonicalrouting.RetiredConnectMigrationBlocker
		want    string
	}{
		{name: "many", blocker: canonicalrouting.RetiredConnectDeliveryMany, want: "only removes delivery: one"},
		{name: "broadcast", blocker: canonicalrouting.RetiredConnectDeliveryBroadcast, want: "only removes delivery: one"},
		{name: "reply delivery", blocker: canonicalrouting.RetiredConnectDeliveryReply, want: "only removes delivery: one"},
		{name: "reply map", blocker: canonicalrouting.RetiredConnectReplyMap, want: "requires manual migration"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := canonicalrouting.RepoRoot(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
			canonicalrouting.ApplyRetiredConnectMigrationBlocker(t, root, tc.blocker)
			packageFile := filepath.Join(root, "package.yaml")
			before, err := os.ReadFile(packageFile)
			if err != nil {
				t.Fatalf("read package.yaml before migration: %v", err)
			}

			var out bytes.Buffer
			var errOut bytes.Buffer
			code := executeRootCommand(context.Background(), repoRoot, []string{
				"migrate-connect-delivery-one",
				"--contracts", root,
			}, &out, &errOut)
			if code == 0 || !strings.Contains(errOut.String(), tc.want) {
				t.Fatalf("migrate code/stderr = %d/%q, want failure containing %q", code, errOut.String(), tc.want)
			}
			after, err := os.ReadFile(packageFile)
			if err != nil {
				t.Fatalf("read package.yaml after migration: %v", err)
			}
			if !bytes.Equal(after, before) {
				t.Fatalf("blocked migration changed package.yaml\nbefore:\n%s\nafter:\n%s", before, after)
			}
		})
	}
}
