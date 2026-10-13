package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

const catalog = "scalars:\n  Label: text\n  ID: uuid\n  Time: timestamp\nenums:\n  Status: {values: [open, closed], default: open}\ntypes:\n  Item:\n    id: text\n    note: text?\n"

func errText(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func main() {
	repo, evidence, binary := os.Args[1], os.Args[2], os.Args[3]
	out := json.NewEncoder(os.Stdout)
	emit := func(row map[string]any) {
		if err := out.Encode(row); err != nil {
			panic(err)
		}
	}
	load := func(ref, extra string, template bool) (*c.WorkflowContractBundle, string, error) {
		root, err := os.MkdirTemp(evidence, "b-source-")
		if err != nil {
			panic(err)
		}
		files := map[string]string{
			"schema.yaml":   "name: b-admission-probe\nstages: {done: {final: true}}\n",
			"types.yaml":    catalog + extra,
			"entities.yaml": fmt.Sprintf("person:\n  items:\n    type: %q\n    _unused_reason: admission-probe\n    _unused_reader_reason: admission-probe\n", ref),
		}
		if ref == "[Chain]" {
			files["types.yaml"] = strings.Replace(catalog, "  Label: text\n", "  Label: text\n  Chain: Label\n", 1)
		}
		if template {
			delete(files, "entities.yaml")
			files["worker/schema.yaml"] = "name: worker\ninstance: id\nstages: {done: {final: true}}\n"
			files["worker/entities.yaml"] = fmt.Sprintf("person:\n  id: %q\n", ref)
		}
		for name, body := range files {
			path := filepath.Join(root, name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				panic(err)
			}
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				panic(err)
			}
		}
		bundle, err := c.LoadWorkflowContractBundleWithOverrides(repo, root, c.DefaultPlatformSpecFile(repo))
		return bundle, root, err
	}
	for _, ref := range []string{"[text]", "[Label]", "map[text]Item", "map[Label]Item", "map[integer]Item", "map[numeric]Item", "map[boolean]Item", "map[uuid]Item", "map[ID]Item", "map[Status]Item", "[map[text]Item]", "map[text][Item]", "Item[]", "[]Item", "[Missing]", "[Cycle]", "[Left]", "[Chain]"} {
		extra := ""
		if ref == "[Cycle]" {
			extra = "  Cycle:\n    child: Cycle\n"
		}
		if ref == "[Left]" {
			extra = "  Left:\n    right: '[Right]'\n  Right:\n    left: map[text]Left\n"
		}
		bundle, root, err := load(ref, extra, false)
		row := map[string]any{"surface": "authored/canonical/singleton/public-verify", "type_ref": ref, "load_error": errText(err)}
		if err == nil {
			primary, err := bundle.ResolveRootPrimaryEntity()
			if err != nil {
				panic(err)
			}
			shape, err := (c.CatalogTypeReference{Type: ref, Catalog: primary.Types}).Resolve()
			row["kind"], row["resolve_error"] = shape.Kind, errText(err)
			if shape.Key != nil {
				row["key_kind"], row["key_name"], row["key_format"] = shape.Key.Kind, shape.Key.Name, shape.Key.Format
			}
			_, err = c.SingletonContainedFields(primary)
			row["singleton_error"] = errText(err)
		}
		command := exec.Command(binary, "verify", "--portable", "--json", root)
		command.Dir = repo
		for _, value := range os.Environ() {
			if !strings.HasPrefix(value, "SWARM_") {
				command.Env = append(command.Env, value)
			}
		}
		raw, err := command.CombinedOutput()
		row["verify_process_error"], row["verify_output"] = errText(err), string(raw)
		emit(row)
	}
	for _, ref := range []string{"text", "Label", "integer", "numeric", "uuid", "ID", "timestamp", "Time", "Status", "Item", "[Item]", "Missing"} {
		bundle, _, err := load(ref, "", true)
		row := map[string]any{"surface": "authored/template", "type_ref": ref, "load_error": errText(err)}
		if err == nil {
			_, err = bundle.ResolveFlowTemplateInstance("worker")
			row["template_error"] = errText(err)
		}
		emit(row)
	}
	for _, pair := range [][2]string{{"empty", ""}, {"unknown", "Missing"}, {"chain", "Other"}, {"cycle", "Loop"}} {
		types := c.TypeCatalogDocument{Scalars: map[string]c.ScalarTypeDecl{"Label": {Base: pair[1]}, "Other": {Base: "text"}, "Loop": {Base: "Loop"}}}
		_, err := (c.CatalogTypeReference{Type: "Label", Catalog: types}).Resolve()
		emit(map[string]any{"surface": "defensive-unadmitted-alias-control", "case": pair[0], "error": errText(err)})
	}
	for _, keyType := range []string{"text", "Label", "integer", "numeric", "boolean", "uuid", "ID", "Status"} {
		contract := entityruntime.Contract{Types: c.TypeCatalogDocument{Scalars: map[string]c.ScalarTypeDecl{"Label": {Base: "text"}, "ID": {Base: "uuid"}}, Enums: map[string]c.EnumTypeDecl{"Status": {Values: []string{"open", "closed"}, Default: "open"}}}}
		for _, key := range []any{"2", 2, " \topen\t ", " \tD9428888-122B-11E1-B85C-61CD3CBB3210\t "} {
			value, err := entityruntime.NormalizeContainedOperationKey(contract, keyType, key)
			emit(map[string]any{"surface": "contained-key-owner", "key_type": keyType, "input": key, "normalized": value, "error": errText(err)})
		}
	}
	projection, err := c.AdmitCollectionProjection(c.CatalogTypeReference{Type: "map[text]integer"})
	if err != nil {
		panic(err)
	}
	items, err := projection.Project(map[string]any{"10": 10, "2": 2, "1": 1, "02": 2, "-1": -1, "+2": 2, " 2": 2})
	emit(map[string]any{"surface": "canonical-map-iteration", "items": items, "error": errText(err)})
}
