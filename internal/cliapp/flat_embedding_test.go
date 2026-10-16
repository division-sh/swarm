package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
)

func TestFlatEmbeddingVerifyAndDescribe(t *testing.T) {
	root := t.TempDir()
	for file, body := range map[string]string{
		"schema.yaml": "name: embedding\npins:\n  inputs: [work.ready]\n  outputs: [work.done]\nstages:\n  active: {}\n",
		"types.yaml":  "types:\n  Marker:\n    marker: {type: text, pattern: '^[a-z]+$', description: Named marker}\n",
		"state.yaml":  "test_entity:\n  _embed: [Marker]\n",
		"events.yaml": "work.ready:\n  _description: Embedded input\n  _embed: [Marker]\n  _key: marker\nwork.done:\n  _embed: [Marker]\n",
		"nodes.yaml":  "node-a:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready:\n      data_accumulation:\n        writes:\n          - target_field: marker\n            value: payload.marker\n      emit:\n        event: work.done\n        fields:\n          marker: entity.marker\n",
	} {
		writeDescribeTestFile(t, filepath.Join(root, file), body)
	}
	var out, diagnostics bytes.Buffer
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--json"}, &out, &diagnostics, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("verify embedded pins/CEL/emit: code=%d out=%s err=%s", code, out.String(), diagnostics.String())
	}
	out.Reset()
	diagnostics.Reset()
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root, "--json"}, &out, &diagnostics, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe embedded source: code=%d out=%s err=%s", code, out.String(), diagnostics.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(out.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.Root.PrimaryEntity == nil || len(view.Root.Events) != 2 {
		t.Fatalf("missing effective declarations: %+v", view.Root)
	}
	stateOrigin := view.Root.PrimaryEntity.FieldOrigins["marker"]
	if stateOrigin.SourceFile != "types.yaml" || stateOrigin.SourceLine != 3 {
		t.Fatalf("state defining coordinate lost: %+v", stateOrigin)
	}
	found := false
	for _, event := range view.Root.Events {
		if event.Name == "work.ready" {
			found = event.Description == "Embedded input" && event.FieldOrigins["marker"].SourceLine == 3
		}
	}
	if !found {
		t.Fatalf("description or event origin lost: %+v", view.Root.Events)
	}
	out.Reset()
	diagnostics.Reset()
	if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root}, &out, &diagnostics, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("text describe: code=%d err=%s", code, diagnostics.String())
	}
	for _, want := range []string{"Embedded input", "root primary entity: test_entity", "marker: derived from _embed[0] at types.yaml:3:"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("text describe missing %q: %s", want, out.String())
		}
	}
}
