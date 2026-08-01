package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestMigrateProducerRoutingRemovesAllDeterministicBroadcasts(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := writeProducerRoutingCodemodBundle(t, map[string]string{
		"nodes.yaml": producerRoutingNodes(`
    first:
      emit:
        event: task.first
        broadcast: true
    second:
      emit:
        event: task.second
        broadcast: true`),
		"flows/worker/nodes.yaml": producerRoutingNodes(`
    work:
      emit:
        event: task.worked
        broadcast: true`),
	})

	var out, errOut bytes.Buffer
	code := executeRootCommand(context.Background(), repo, []string{"migrate-producer-routing", "--contracts", root}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "removed 3 retired emit.broadcast: true declarations") {
		t.Fatalf("migrate code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
	}
	for _, relative := range []string{"nodes.yaml", "flows/worker/nodes.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "broadcast:") {
			t.Fatalf("%s retains broadcast:\n%s", relative, raw)
		}
	}

	out.Reset()
	errOut.Reset()
	code = executeRootCommand(context.Background(), repo, []string{"migrate-producer-routing", "--contracts", root}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "no deterministic emit.broadcast: true declarations found") {
		t.Fatalf("idempotent migrate code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
	}
}

func TestMigrateProducerRoutingRejectsManualCasesWithoutChangingFile(t *testing.T) {
	for _, tc := range []struct {
		name    string
		blocker string
		want    string
	}{
		{name: "sender target", blocker: "target: sender", want: "emit.target"},
		{name: "instance target", blocker: "target: {instance_id: worker-1}", want: "emit.target"},
		{name: "flow match target", blocker: "target: {flow: worker, match: {worker_id: payload.worker_id}}", want: "emit.target"},
		{name: "broadcast false", blocker: "broadcast: false", want: "not boolean true"},
		{name: "broadcast null", blocker: "broadcast: null", want: "not boolean true"},
		{name: "broadcast malformed", blocker: "broadcast: [true]", want: "not boolean true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := writeProducerRoutingCodemodBundle(t, map[string]string{
				"nodes.yaml": producerRoutingNodes("\n    deterministic:\n      emit:\n        event: task.done\n        broadcast: true\n    manual:\n      emit:\n        event: task.manual\n        " + tc.blocker),
			})
			path := filepath.Join(root, "nodes.yaml")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			var out, errOut bytes.Buffer
			code := executeRootCommand(context.Background(), repo, []string{"migrate-producer-routing", "--contracts", root}, &out, &errOut)
			if code == 0 || !strings.Contains(errOut.String(), tc.want) {
				t.Fatalf("migrate code/stderr = %d/%q, want %q", code, errOut.String(), tc.want)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("blocked migration changed nodes.yaml\nbefore:\n%s\nafter:\n%s", before, after)
			}
		})
	}
}

func TestMigrateProducerRoutingPreflightsWholeBundleBeforeWriting(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := writeProducerRoutingCodemodBundle(t, map[string]string{
		"nodes.yaml": producerRoutingNodes(`
    deterministic:
      emit:
        event: task.done
        broadcast: true`),
		"z-flow/nodes.yaml": producerRoutingNodes(`
    manual:
      emit:
        event: task.manual
        target: sender`),
	})
	before := map[string][]byte{}
	for _, relative := range []string{"nodes.yaml", "z-flow/nodes.yaml"} {
		before[relative], _ = os.ReadFile(filepath.Join(root, relative))
	}

	var out, errOut bytes.Buffer
	code := executeRootCommand(context.Background(), repo, []string{"migrate-producer-routing", "--contracts", root}, &out, &errOut)
	if code == 0 || !strings.Contains(errOut.String(), "emit.target") {
		t.Fatalf("migrate code/stderr = %d/%q, want manual target rejection", code, errOut.String())
	}
	for relative, want := range before {
		got, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("bundle preflight changed %s\nbefore:\n%s\nafter:\n%s", relative, want, got)
		}
	}
}

func TestMigrateProducerRoutingIgnoresNestedLiteralEmitMap(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := writeProducerRoutingCodemodBundle(t, map[string]string{
		"nodes.yaml": producerRoutingNodes(`
    request:
      emit:
        event: task.done
        fields:
          config:
            literal:
              emit:
                broadcast: true`),
	})
	path := filepath.Join(root, "nodes.yaml")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	code := executeRootCommand(context.Background(), repo, []string{"migrate-producer-routing", "--contracts", root}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "no deterministic emit.broadcast: true declarations found") {
		t.Fatalf("migrate code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("nested literal was rewritten\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestMigrateProducerRoutingCoversEveryStructuredNestedEmitSurface(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := writeProducerRoutingCodemodBundle(t, map[string]string{
		"nodes.yaml": producerRoutingNodes(`
    nested:
      on_success:
        emit: {event: task.success, broadcast: true}
      rules:
        - id: direct
          emit: {event: task.rule, broadcast: true}
        - id: expanded
          fan_out:
            items_from: payload.items
            as: item
            identity: item.id
            emit: {event: task.rule_item, broadcast: true}
      on_complete:
        emit: {event: task.complete, broadcast: true}
      join:
        stage: waiting
        members: {from: entity.ids, by: payload.id}
        output: payload.result
        on_complete:
          emit: {event: task.joined, broadcast: true}
        timeout:
          after: 1h
          emit: {event: task.timed_out, broadcast: true}`),
	})

	var out, errOut bytes.Buffer
	code := executeRootCommand(context.Background(), repo, []string{"migrate-producer-routing", "--contracts", root}, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "removed 6 retired emit.broadcast: true declarations") {
		t.Fatalf("migrate code/stdout/stderr = %d/%q/%q", code, out.String(), errOut.String())
	}
	raw, err := os.ReadFile(filepath.Join(root, "nodes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if retired, err := runtimecontracts.HasRetiredProducerRoutingYAML(raw); err != nil || retired {
		t.Fatalf("retired routing after codemod = %v, err = %v\n%s", retired, err, raw)
	}
}

func writeProducerRoutingCodemodBundle(t testing.TB, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "package.yaml"), []byte("name: producer-routing-codemod\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for relative, raw := range files {
		path := filepath.Join(root, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func producerRoutingNodes(handlers string) string {
	return "worker:\n  id: worker\n  event_handlers:" + handlers + "\n"
}
