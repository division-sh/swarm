// Command rewrite-node-shape-2293 rewrites explicitly selected node declarations,
// not data files or historical snapshots. Dry run is the default.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

type rewriteCounts struct {
	Subscriptions, ExecutionTypes, Produces, HandlerContainers int
}

func (c rewriteCounts) changed() bool {
	return c.Subscriptions+c.ExecutionTypes+c.Produces+c.HandlerContainers != 0
}

func rewriteNodeSource(source []byte) ([]byte, rewriteCounts, error) {
	snapshot, err := yamlsource.Load(source)
	if err != nil {
		return nil, rewriteCounts{}, err
	}
	root, ok := snapshot.Root().Child(0)
	if !ok || root.Kind() != yamlsource.MappingNode {
		return nil, rewriteCounts{}, fmt.Errorf("expected complete nodes.yaml declarations")
	}
	value := snapshot.Document("nodes.yaml").Root()
	if err := value.ValidateAcyclic(); err != nil {
		return nil, rewriteCounts{}, err
	}
	if err := value.ValidateExpansion(); err != nil {
		return nil, rewriteCounts{}, err
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return nil, rewriteCounts{}, err
	}
	// Edit retained source spans, never re-emit maps or change CEL scalar styles.
	lines := strings.SplitAfter(string(source), "\n")
	type lineEdit struct {
		remove  bool
		dedent  int
		replace string
	}
	edits := make(map[int]lineEdit)
	ends := make(map[yamlsource.Node]int)
	for i := 0; i < root.Len(); i += 2 {
		node, _ := root.Child(i + 1)
		end := len(lines)
		if next, ok := root.Child(i + 2); ok {
			end = next.Line() - 1
		}
		ends[node] = end
	}
	indent := func(line string) int { return len(line) - len(strings.TrimLeft(line, " ")) }
	var counts rewriteCounts
	seen := make(map[yamlsource.Node]bool)
	var rewrite func(yamlsource.Node) error
	rewrite = func(node yamlsource.Node) error {
		if seen[node] {
			return nil
		}
		if target, alias := node.Alias(); alias {
			if err := rewrite(target); err != nil {
				return err
			}
			seen[node] = true
			return nil
		}
		if node.Kind() != yamlsource.MappingNode {
			return fmt.Errorf("node declaration at line %d is not a mapping", node.Line())
		}
		end, owned := ends[node]
		if !owned {
			return fmt.Errorf("node at line %d needs an explicit source-ledger rewrite (anchor outside a node declaration)", node.Line())
		}
		remainingFields := 0
		hasContainer := false
		for i := 0; i < node.Len(); i += 2 {
			key, _ := node.Child(i)
			switch key.Value() {
			case "event_handlers":
				hasContainer = true
			case "subscribes_to", "execution_type", "produces":
			default:
				remainingFields++
			}
		}
		for i := 0; i < node.Len(); i += 2 {
			key, _ := node.Child(i)
			field, _ := node.Child(i + 1)
			switch key.Value() {
			case "<<":
				if field.Kind() == yamlsource.SequenceNode {
					for j := 0; j < field.Len(); j++ {
						member, _ := field.Child(j)
						if err := rewrite(member); err != nil {
							return err
						}
					}
				} else if err := rewrite(field); err != nil {
					return err
				}
				continue
			case "subscribes_to", "execution_type", "produces", "event_handlers":
			default:
				continue
			}
			start := key.Line() - 1
			stop := end
			if next, ok := node.Child(i + 2); ok {
				stop = next.Line() - 1
			}
			line := strings.TrimSpace(lines[start])
			prefix := key.Value() + ":"
			if !strings.HasPrefix(line, prefix) || stop <= start {
				return fmt.Errorf("%s at line %d needs an explicit source-ledger rewrite (inline or quoted field)", key.Value(), key.Line())
			}
			if key.Value() != "event_handlers" {
				for j := start; j < stop; j++ {
					edits[j] = lineEdit{remove: true}
				}
				switch key.Value() {
				case "subscribes_to":
					counts.Subscriptions++
				case "execution_type":
					counts.ExecutionTypes++
				case "produces":
					counts.Produces++
				}
				continue
			}
			tail := strings.TrimSpace(strings.TrimPrefix(line, prefix))
			if field.Kind() != yamlsource.MappingNode || (tail != "" && tail != "{}" && !strings.HasPrefix(tail, "#")) {
				return fmt.Errorf("event_handlers at line %d needs an explicit source-ledger rewrite (alias, anchor, inline or nonmapping)", key.Line())
			}
			counts.HandlerContainers++
			edits[start] = lineEdit{remove: true}
			if strings.HasPrefix(tail, "#") {
				edits[start] = lineEdit{replace: strings.Repeat(" ", indent(lines[start])) + tail + "\n"}
			}
			if field.Len() == 0 {
				if remainingFields == 0 {
					edits[start] = lineEdit{replace: strings.Repeat(" ", indent(lines[start])) + "{}\n"}
				}
				continue
			}
			first, _ := field.Child(0)
			delta := indent(lines[first.Line()-1]) - indent(lines[start])
			if delta <= 0 {
				return fmt.Errorf("handler container at line %d is not block-indented", key.Line())
			}
			for j := start + 1; j < stop; j++ {
				if strings.TrimSpace(lines[j]) != "" && indent(lines[j]) >= delta {
					edits[j] = lineEdit{dedent: delta}
				}
			}
		}
		if remainingFields == 0 && !hasContainer && node.Len() != 0 {
			first, _ := node.Child(0)
			line := first.Line() - 1
			edits[line] = lineEdit{replace: strings.Repeat(" ", indent(lines[line])) + "{}\n"}
		}
		seen[node] = true
		return nil
	}
	for i := 0; i < root.Len(); i += 2 {
		key, _ := root.Child(i)
		node, _ := root.Child(i + 1)
		if err := rewrite(node); err != nil {
			return nil, rewriteCounts{}, fmt.Errorf("node %q: %w", key.Value(), err)
		}
	}
	if !counts.changed() {
		return source, counts, nil
	}
	var output strings.Builder
	for i, line := range lines {
		edit := edits[i]
		switch {
		case edit.remove:
			continue
		case edit.replace != "":
			output.WriteString(edit.replace)
		default:
			output.WriteString(line[edit.dedent:])
		}
	}
	result := []byte(output.String())
	projected, err := yamlsource.Load(result)
	if err != nil {
		return nil, rewriteCounts{}, err
	}
	if err := projected.Document("nodes.yaml").Root().ValidateUniqueMappings(); err != nil {
		return nil, rewriteCounts{}, err
	}
	return result, counts, nil
}

func run(paths []string, write bool) error {
	if len(paths) == 0 {
		return fmt.Errorf("select nodes.yaml paths explicitly; no implicit corpus or history traversal")
	}
	type plannedFile struct {
		path   string
		output []byte
		counts rewriteCounts
		mode   os.FileMode
	}
	var plan []plannedFile
	for _, path := range paths {
		if filepath.Base(path) != "nodes.yaml" {
			return fmt.Errorf("%s is not nodes.yaml; generated sources require their explicit ledger", path)
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		output, counts, err := rewriteNodeSource(source)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		plan = append(plan, plannedFile{path: path, output: output, counts: counts, mode: info.Mode().Perm()})
	}
	// Validate the whole selected set before applying any source changes.
	for _, file := range plan {
		if write && file.counts.changed() {
			if err := os.WriteFile(file.path, file.output, file.mode); err != nil {
				return err
			}
		}
		fmt.Printf("%s: subscriptions=%d execution_types=%d produces=%d handler_containers=%d write=%v\n",
			filepath.ToSlash(file.path), file.counts.Subscriptions, file.counts.ExecutionTypes, file.counts.Produces, file.counts.HandlerContainers, write)
	}
	return nil
}

func main() {
	write := flag.Bool("write", false, "apply the selected node shape rewrite")
	activities := flag.Bool("capture-activities", false, "read old effective activity identities from explicitly selected source bundle roots")
	disk := flag.Bool("prepare-disk", false, "capture immutable before inputs and replay hashes for the selected node files")
	goInputs := flag.Bool("prepare-go", false, "capture an explicit ledger of complete node literals in selected Go functions")
	goPlan := flag.String("go-plan", "", "apply only the reviewed Go literal ledger (dry run unless -write)")
	flag.Parse()
	err := func() error {
		if *goInputs || *goPlan != "" {
			if *disk || *activities || *goInputs && (*write || *goPlan != "") || *goPlan != "" && flag.NArg() != 0 {
				return fmt.Errorf("Go literal rewriting is a separate explicit operation")
			}
			if *goPlan != "" {
				return applyGoInputs(*goPlan, *write)
			}
			plan, err := prepareGoInputs(flag.Args())
			if err != nil {
				return err
			}
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(plan)
		}
		if *disk {
			if *write || *activities {
				return fmt.Errorf("disk input preparation is a separate read-only operation")
			}
			plan, err := prepareDiskInputs(flag.Args())
			if err != nil {
				return err
			}
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(plan)
		}
		if !*activities {
			return run(flag.Args(), *write)
		}
		if *write {
			return fmt.Errorf("activity identity capture is read-only")
		}
		repo, err := os.Getwd()
		if err != nil {
			return err
		}
		pins, err := captureActivityIdentities(repo, flag.Args())
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(pins)
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
		os.Exit(1)
	}
}
