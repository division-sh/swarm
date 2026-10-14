// Command rewrite-node-shape-2293 rewrites explicitly selected node declarations,
// not data files or historical snapshots. Dry run is the default.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

type rewriteCounts struct {
	Subscriptions, ExecutionTypes, Produces, HandlerContainers int
}

func (c rewriteCounts) changed() bool {
	return c.Subscriptions+c.ExecutionTypes+c.Produces+c.HandlerContainers != 0
}

func rewriteNodeSource(source []byte) ([]byte, rewriteCounts, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(source, &document); err != nil {
		return nil, rewriteCounts{}, err
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, rewriteCounts{}, fmt.Errorf("expected complete nodes.yaml declarations")
	}
	value := yamlsource.ValueFromNode(document.Content[0])
	if err := value.ValidateAcyclic(); err != nil {
		return nil, rewriteCounts{}, err
	}
	if err := value.ValidateExpansion(); err != nil {
		return nil, rewriteCounts{}, err
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return nil, rewriteCounts{}, err
	}
	var counts rewriteCounts
	seen := make(map[*yaml.Node]bool)
	var rewrite func(*yaml.Node) error
	rewrite = func(node *yaml.Node) error {
		if seen[node] {
			return nil
		}
		if node.Kind == yaml.AliasNode {
			if err := rewrite(node.Alias); err != nil {
				return err
			}
			seen[node] = true
			return nil
		}
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("node declaration at line %d is not a mapping", node.Line)
		}
		var next []*yaml.Node
		for i := 0; i < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			switch key.Value {
			case "subscribes_to":
				counts.Subscriptions++
			case "execution_type":
				counts.ExecutionTypes++
			case "produces":
				counts.Produces++
			case "event_handlers":
				// A wrapper anchor names only the handler map, not the whole node;
				// moving it to the node would silently change alias semantics.
				if value.Kind != yaml.MappingNode || value.Anchor != "" {
					return fmt.Errorf("event_handlers at line %d needs an explicit source-ledger rewrite (alias, anchor or nonmapping)", key.Line)
				}
				counts.HandlerContainers++
				next = append(next, value.Content...)
			case "<<":
				if value.Kind == yaml.SequenceNode {
					for _, member := range value.Content {
						if err := rewrite(member); err != nil {
							return err
						}
					}
				} else if err := rewrite(value); err != nil {
					return err
				}
				next = append(next, key, value)
			default:
				next = append(next, key, value)
			}
		}
		node.Content = next
		seen[node] = true
		return nil
	}
	root := document.Content[0]
	for i := 0; i < len(root.Content); i += 2 {
		if err := rewrite(root.Content[i+1]); err != nil {
			return nil, rewriteCounts{}, fmt.Errorf("node %q: %w", root.Content[i].Value, err)
		}
	}
	if err := yamlsource.ValueFromNode(root).ValidateUniqueMappings(); err != nil {
		return nil, rewriteCounts{}, err
	}
	if !counts.changed() {
		return source, counts, nil
	}
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return nil, rewriteCounts{}, err
	}
	if err := encoder.Close(); err != nil {
		return nil, rewriteCounts{}, err
	}
	return output.Bytes(), counts, nil
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
	flag.Parse()
	if err := run(flag.Args(), *write); err != nil {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
		os.Exit(1)
	}
}
