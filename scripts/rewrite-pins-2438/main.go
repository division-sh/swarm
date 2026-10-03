// One-shot source rewrite for the ratified #2438 slice 2b corpus, not a runtime reader.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"gopkg.in/yaml.v3"
)

const replyRequester = "examples/routing/template-reply/requester/schema.yaml"
const replyParent = "examples/routing/template-reply/schema.yaml"
const retiredHarness = "examples/routing/harness-injection/"

var retiredGrantSchemas = map[string]bool{
	"internal/runtime/testdata/generic-swarm-bundle/intake/schema.yaml":             true,
	"internal/runtime/testdata/generic-swarm-bundle/processing/schema.yaml":         true,
	"internal/runtime/testdata/generic-swarm-bundle/delivery/schema.yaml":           true,
	"tests/tier11-flow-composition/test-data-pin-wiring/schema.yaml":                true,
	"tests/tier11-flow-composition/test-data-pin-wiring/processor/schema.yaml":      true,
	"tests/tier11-flow-composition/test-data-pin-write-conflict/flow-a/schema.yaml": true,
	"tests/tier11-flow-composition/test-data-pin-write-conflict/flow-b/schema.yaml": true,
}

type change struct {
	Path   string `json:"path"`
	Delete bool   `json:"delete,omitempty"`
	After  []byte `json:"-"`
}

type source struct {
	data []byte
	root *yaml.Node
	edit map[string]*yaml.Node
}

type reply struct {
	event       string
	request     *yaml.Node
	correlation *yaml.Node
}

func main() {
	root := flag.String("root", ".", "repository checkout")
	write := flag.Bool("write", false, "apply the complete planned corpus rewrite")
	flag.Parse()
	if err := run(*root, *write); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root string, write bool) error {
	files, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		return err
	}
	inputs := map[string][]byte{}
	for _, name := range strings.Split(string(files), "\x00") {
		if !schemaFile(name) && !strings.HasPrefix(name, retiredHarness) {
			continue
		}
		file := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(file)
		if os.IsNotExist(err) && (retiredGrantSchemas[name] || strings.HasPrefix(name, retiredHarness)) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%s: expected a regular tracked file: %v", name, err)
		}
		inputs[name], err = os.ReadFile(file)
		if err != nil {
			return err
		}
	}
	plan, err := planRewrite(inputs)
	if err != nil {
		return err
	}
	if write {
		for _, item := range plan {
			file := filepath.Join(root, filepath.FromSlash(item.Path))
			if item.Delete {
				err = os.Remove(file)
			} else {
				info, statErr := os.Stat(file)
				if statErr != nil {
					return statErr
				}
				err = os.WriteFile(file, item.After, info.Mode().Perm())
			}
			if err != nil {
				return err
			}
		}
	}
	return json.NewEncoder(os.Stdout).Encode(plan)
}

func schemaFile(name string) bool {
	return path.Base(name) == "schema.yaml" || path.Base(name) == "schema.yml"
}

func planRewrite(inputs map[string][]byte) ([]change, error) {
	docs := map[string]*source{}
	var plan []change
	for name, data := range inputs {
		if strings.HasPrefix(name, retiredHarness) || retiredGrantSchemas[name] {
			plan = append(plan, change{Path: name, Delete: true})
			continue
		}
		doc, err := parse(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		docs[name] = doc
	}
	for name, doc := range docs {
		pins, err := lookup(doc.root, "pins")
		if err != nil || pins == nil {
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			continue
		}
		converted, replies, err := rewritePins(pins)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		doc.edit["pins"] = converted
		for _, item := range replies {
			if name != replyRequester || docs[replyParent] == nil {
				return nil, fmt.Errorf("%s: reply lies outside the ratified corpus row", name)
			}
			parent := docs[replyParent]
			if err := moveReply(parent, item); err != nil {
				return nil, fmt.Errorf("%s: %w", replyParent, err)
			}
		}
	}
	for name, doc := range docs {
		after, err := render(doc)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if !bytes.Equal(doc.data, after) {
			plan = append(plan, change{Path: name, After: after})
		}
	}
	sort.Slice(plan, func(i, j int) bool { return plan[i].Path < plan[j].Path })
	return plan, nil
}

func parse(data []byte) (*source, error) {
	var doc yaml.Node
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&doc); err != nil {
		return nil, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("expected exactly one YAML document: %v", err)
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("expected schema mapping")
	}
	if err := plainTree(doc.Content[0]); err != nil {
		return nil, err
	}
	return &source{data: data, root: doc.Content[0], edit: map[string]*yaml.Node{}}, nil
}

func plainTree(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode || node.Anchor != "" {
		return fmt.Errorf("one-shot rewrite does not expand YAML aliases or anchors")
	}
	if node.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || seen[key.Value] {
				return fmt.Errorf("invalid or duplicate mapping key %q", key.Value)
			}
			seen[key.Value] = true
		}
	}
	for _, child := range node.Content {
		if err := plainTree(child); err != nil {
			return err
		}
	}
	return nil
}

func lookup(node *yaml.Node, key string) (*yaml.Node, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("expected mapping for %s", key)
	}
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1], nil
		}
	}
	return nil, nil
}

func rewritePins(pins *yaml.Node) (*yaml.Node, []reply, error) {
	if pins.Kind != yaml.MappingNode {
		return nil, nil, fmt.Errorf("expected pins mapping")
	}
	result := *pins
	result.Content = nil
	var replies []reply
	for i := 0; i < len(pins.Content); i += 2 {
		key, direction := pins.Content[i], pins.Content[i+1]
		if key.Value != "inputs" && key.Value != "outputs" {
			return nil, nil, fmt.Errorf("unknown pins direction %q", key.Value)
		}
		if direction.Kind == yaml.MappingNode {
			if len(direction.Content) != 2 || direction.Content[0].Value != "events" {
				return nil, nil, fmt.Errorf("unexpected pin wrapper; grant fixtures must be retired explicitly")
			}
			direction = direction.Content[1]
		}
		if direction.Kind != yaml.SequenceNode {
			return nil, nil, fmt.Errorf("%s: expected event sequence", key.Value)
		}
		list := *direction
		list.Content = nil
		for _, item := range direction.Content {
			converted, paired, err := rewritePin(item, key.Value)
			if err != nil {
				return nil, nil, err
			}
			list.Content = append(list.Content, converted)
			if paired != nil {
				replies = append(replies, *paired)
			}
		}
		result.Content = append(result.Content, key, &list)
	}
	return &result, replies, nil
}

func rewritePin(item *yaml.Node, direction string) (*yaml.Node, *reply, error) {
	if item.Kind == yaml.ScalarNode {
		return item, nil, validEvent(item)
	}
	event, err := lookup(item, "event")
	if err != nil || event == nil {
		return nil, nil, fmt.Errorf("expected event pin: %v", err)
	}
	if err := validEvent(event); err != nil {
		return nil, nil, err
	}
	var initialize *yaml.Node
	var paired *reply
	for i := 0; i < len(item.Content); i += 2 {
		key, value := item.Content[i].Value, item.Content[i+1]
		switch {
		case key == "event":
		case key == "source" && direction == "inputs", key == "sink" && direction == "outputs":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!str" || value.Value != "harness" {
				return nil, nil, fmt.Errorf("unexpected %s value", key)
			}
		case key == "initialize" && direction == "inputs":
			initialize = value
		case key == "resolution" && direction == "inputs":
			paired, err = replyFrom(value, event.Value)
			if err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, fmt.Errorf("unknown %s pin key %q", direction, key)
		}
	}
	if initialize != nil {
		if paired != nil || initialize.Kind != yaml.MappingNode {
			return nil, nil, fmt.Errorf("unexpected combined reply/initialization or initialization shape")
		}
		copy := *item
		copy.Content = []*yaml.Node{textNode("event"), event, textNode("initialize"), initialize}
		return &copy, nil, nil
	}
	copy := *event
	copy.HeadComment = item.HeadComment
	return &copy, paired, nil
}

func validEvent(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" || !eventidentity.IsValidName(node.Value) {
		return fmt.Errorf("invalid event %q", node.Value)
	}
	return nil
}

func replyFrom(resolution *yaml.Node, event string) (*reply, error) {
	mode, err := lookup(resolution, "mode")
	if err != nil || mode == nil || mode.Value != "reply" || mode.Tag != "!!str" {
		return nil, fmt.Errorf("unexpected pin resolution; only the ratified reply row is rewritten")
	}
	for i := 0; i < len(resolution.Content); i += 2 {
		key := resolution.Content[i].Value
		if key != "mode" && key != "replies_to" && key != "correlation_key" {
			return nil, fmt.Errorf("unknown reply key %q", key)
		}
	}
	request, _ := lookup(resolution, "replies_to")
	if request == nil {
		return nil, fmt.Errorf("missing reply request")
	}
	if err := validEvent(request); err != nil {
		return nil, err
	}
	correlation, _ := lookup(resolution, "correlation_key")
	if correlation != nil && (correlation.Kind != yaml.ScalarNode || correlation.Tag != "!!str" || correlation.Value == "") {
		return nil, fmt.Errorf("invalid correlation key")
	}
	return &reply{event: event, request: request, correlation: correlation}, nil
}

func moveReply(parent *source, item reply) error {
	connections, err := lookup(parent.root, "connect")
	if err != nil || connections == nil || connections.Kind != yaml.SequenceNode {
		return fmt.Errorf("missing exact reply connections")
	}
	response, err := connection(connections, "", "requester", item.event)
	if err != nil {
		return err
	}
	provider, _ := lookup(response, "from")
	if provider == nil || provider.Tag != "!!str" {
		return fmt.Errorf("missing provider identity")
	}
	if _, err := connection(connections, "requester", provider.Value, item.request.Value); err != nil {
		return err
	}
	if existing, _ := lookup(response, "replies_to"); existing != nil {
		return fmt.Errorf("reply already has two declaration owners")
	}
	response.Content = append(response.Content, textNode("replies_to"), item.request)
	if item.correlation != nil {
		if existing, _ := lookup(response, "correlation_key"); existing != nil {
			return fmt.Errorf("correlation already has two declaration owners")
		}
		response.Content = append(response.Content, textNode("correlation_key"), item.correlation)
	}
	parent.edit["connect"] = connections
	return nil
}

func connection(connections *yaml.Node, from, to, event string) (*yaml.Node, error) {
	var matches []*yaml.Node
	for _, row := range connections.Content {
		producer, err := lookup(row, "from")
		if err != nil {
			return nil, err
		}
		receiver, _ := lookup(row, "to")
		name, _ := lookup(row, "event")
		if receiver == nil || producer == nil || name == nil {
			return nil, fmt.Errorf("incomplete connection")
		}
		if renamed, _ := lookup(row, "rename"); renamed != nil && from == "" {
			name = renamed
		}
		if receiver.Value == to && name.Value == event && (from == "" || producer.Value == from) {
			matches = append(matches, row)
		}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("expected one exact %s -> %s connection for %s, got %d", from, to, event, len(matches))
	}
	return matches[0], nil
}

func textNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

func render(doc *source) ([]byte, error) {
	lines := bytes.SplitAfter(doc.data, []byte("\n"))
	var output bytes.Buffer
	cursor := 0
	for i := 0; i < len(doc.root.Content); i += 2 {
		key := doc.root.Content[i]
		value := doc.edit[key.Value]
		if value == nil {
			continue
		}
		start, end := key.Line-1, len(lines)
		if i+2 < len(doc.root.Content) {
			next := doc.root.Content[i+2]
			end = next.Line - 1
			if next.HeadComment != "" {
				end = precedingCommentStart(lines, start, end)
			}
		}
		if key.Column != 1 || start < cursor || end <= start {
			return nil, fmt.Errorf("unsupported inline root shape")
		}
		tail := end
		for tail > start+1 && strings.TrimSpace(string(lines[tail-1])) == "" {
			tail--
		}
		output.Write(bytes.Join(lines[cursor:start], nil))
		copy := *key
		copy.HeadComment = ""
		field := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{&copy, value}}
		encoder := yaml.NewEncoder(&output)
		encoder.SetIndent(2)
		if err := encoder.Encode(field); err != nil {
			return nil, err
		}
		if err := encoder.Close(); err != nil {
			return nil, err
		}
		output.Write(bytes.Join(lines[tail:end], nil))
		cursor = end
	}
	output.Write(bytes.Join(lines[cursor:], nil))
	return output.Bytes(), nil
}

func precedingCommentStart(lines [][]byte, start, end int) int {
	for end > start+1 {
		line := strings.TrimSpace(string(lines[end-1]))
		if line != "" && !strings.HasPrefix(line, "#") {
			break
		}
		end--
	}
	return end
}
