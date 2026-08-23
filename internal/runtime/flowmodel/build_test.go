package flowmodel

import (
	"path/filepath"
	"testing"
)

func TestAssemblePackageTreeDoesNotAttachUnrelatedPackageToSoleFlow(t *testing.T) {
	type flow struct {
		id  string
		dir string
	}
	type pkg struct {
		key    string
		parent string
		dir    string
		flows  []flow
	}
	rootDir := t.TempDir()
	packages := []pkg{
		{key: ".", dir: rootDir, flows: []flow{{id: "support", dir: filepath.Join(rootDir, "flows", "support")}}},
		{key: "extras", parent: ".", dir: filepath.Join(rootDir, "extras")},
	}

	root, err := AssemblePackageTree(
		packages,
		func(value pkg) string { return value.key },
		func(value pkg) string { return value.parent },
		func(value pkg) string { return value.dir },
		func(value pkg) []flow { return value.flows },
		func(value flow) string { return value.id },
		func(value flow) string { return value.id },
		func(value flow) string { return value.dir },
		func(value pkg) *BuildNode[string] { return &BuildNode[string]{View: "package:" + value.key} },
		func(value flow) *BuildNode[string] { return &BuildNode[string]{View: "flow:" + value.id} },
	)
	if err != nil {
		t.Fatalf("AssemblePackageTree: %v", err)
	}
	if len(root.Children) != 2 || root.Children[0].View != "flow:support" || root.Children[1].View != "package:extras" {
		t.Fatalf("root children = %#v, want sole flow and unrelated package as siblings", root.Children)
	}
	if len(root.Children[0].Children) != 0 {
		t.Fatalf("sole flow children = %#v, unrelated package borrowed flow ownership", root.Children[0].Children)
	}
}

func TestAssemblePackageTreeAttachesStructurallyNestedPackageToFlow(t *testing.T) {
	type flow struct {
		id  string
		dir string
	}
	type pkg struct {
		key    string
		parent string
		dir    string
		flows  []flow
	}
	rootDir := t.TempDir()
	flowDir := filepath.Join(rootDir, "flows", "support")
	packages := []pkg{
		{key: ".", dir: rootDir, flows: []flow{{id: "support", dir: flowDir}}},
		{key: "flows/support/extras", parent: ".", dir: filepath.Join(flowDir, "extras")},
	}

	root, err := AssemblePackageTree(
		packages,
		func(value pkg) string { return value.key },
		func(value pkg) string { return value.parent },
		func(value pkg) string { return value.dir },
		func(value pkg) []flow { return value.flows },
		func(value flow) string { return value.id },
		func(value flow) string { return value.id },
		func(value flow) string { return value.dir },
		func(value pkg) *BuildNode[string] { return &BuildNode[string]{View: "package:" + value.key} },
		func(value flow) *BuildNode[string] { return &BuildNode[string]{View: "flow:" + value.id} },
	)
	if err != nil {
		t.Fatalf("AssemblePackageTree: %v", err)
	}
	if len(root.Children) != 1 || root.Children[0].View != "flow:support" || len(root.Children[0].Children) != 1 || root.Children[0].Children[0].View != "package:flows/support/extras" {
		t.Fatalf("tree = %#v, want structurally nested package beneath support", root)
	}
}
