// One-shot source rewrite for the ratified #2438 slice 2b corpus, not a runtime reader.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
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
	root yamlsource.Value
	edit map[string]any
}

type reply struct {
	event       string
	request     string
	correlation string
}

type pinEntry struct {
	Event      string            `yaml:"event"`
	Initialize map[string]string `yaml:"initialize,omitempty"`
}

func (p pinEntry) MarshalYAML() (any, error) {
	if p.Initialize == nil {
		return p.Event, nil
	}
	type initializedPin pinEntry
	return initializedPin(p), nil
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
		pins, err := lookupValue(doc.root, "pins")
		if err != nil || pins.Presence() == yamlsource.PresenceMissing {
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
	snapshot, err := yamlsource.Load(data)
	if err != nil {
		return nil, err
	}
	root := snapshot.Document("schema.yaml").Root()
	if root.Presence() != yamlsource.PresenceMapping {
		return nil, fmt.Errorf("expected schema mapping")
	}
	if err := root.ValidateExpansion(); err != nil {
		return nil, err
	}
	if err := root.ValidateUniqueMappings(); err != nil {
		return nil, err
	}
	if err := plainTree(snapshot.Root()); err != nil {
		return nil, err
	}
	return &source{data: data, root: root, edit: map[string]any{}}, nil
}

func plainTree(node yamlsource.Node) error {
	if node.Kind() == yamlsource.AliasNode {
		return fmt.Errorf("one-shot rewrite does not expand YAML aliases")
	}
	for i := 0; i < node.Len(); i++ {
		child, _ := node.Child(i)
		if node.Kind() == yamlsource.MappingNode && i%2 == 0 && (child.Kind() != yamlsource.ScalarNode || child.Tag() != "!!str") {
			return fmt.Errorf("invalid mapping key %q", child.Value())
		}
		if err := plainTree(child); err != nil {
			return err
		}
	}
	return nil
}

func lookupValue(value yamlsource.Value, key string) (yamlsource.Value, error) {
	field, err := value.Lookup(key)
	return field.Value, err
}

func rewritePins(pins yamlsource.Value) (map[string][]pinEntry, []reply, error) {
	fields, err := pins.Mapping()
	if err != nil {
		return nil, nil, err
	}
	result := map[string][]pinEntry{}
	var replies []reply
	for _, field := range fields {
		if field.Name != "inputs" && field.Name != "outputs" {
			return nil, nil, fmt.Errorf("unknown pins direction %q", field.Name)
		}
		direction := field.Value
		if direction.Presence() == yamlsource.PresenceMapping {
			wrapper, err := direction.Mapping()
			if err != nil || len(wrapper) != 1 || wrapper[0].Name != "events" {
				return nil, nil, fmt.Errorf("unexpected pin wrapper; grant fixtures must be retired explicitly")
			}
			direction = wrapper[0].Value
		}
		items, err := direction.Sequence()
		if err != nil {
			return nil, nil, err
		}
		list := make([]pinEntry, 0, len(items))
		seen := map[string]bool{}
		for _, item := range items {
			converted, paired, err := rewritePin(item, field.Name)
			if err != nil {
				return nil, nil, err
			}
			name := converted.Event
			if seen[name] {
				return nil, nil, fmt.Errorf("duplicate %s event %q", field.Name, name)
			}
			seen[name] = true
			list = append(list, converted)
			if paired != nil {
				replies = append(replies, *paired)
			}
		}
		result[field.Name] = list
	}
	return result, replies, nil
}

func rewritePin(item yamlsource.Value, direction string) (pinEntry, *reply, error) {
	if item.Presence() == yamlsource.PresenceScalar {
		event, err := validEvent(item)
		return pinEntry{Event: event}, nil, err
	}
	fields, err := item.Mapping()
	if err != nil {
		return pinEntry{}, nil, err
	}
	event, _ := lookupValue(item, "event")
	name, err := validEvent(event)
	if err != nil {
		return pinEntry{}, nil, err
	}
	if len(fields) == 1 {
		return pinEntry{}, nil, fmt.Errorf("pin mapping has no non-default option")
	}
	var initialize map[string]string
	var paired *reply
	for _, field := range fields {
		key, value := field.Name, field.Value
		switch {
		case key == "event":
		case key == "source" && direction == "inputs", key == "sink" && direction == "outputs":
			if text, err := exactText(value); err != nil || text != "harness" {
				return pinEntry{}, nil, fmt.Errorf("unexpected %s value", key)
			}
		case key == "initialize" && direction == "inputs":
			initialize, err = textMapping(value)
			if err != nil || len(initialize) == 0 {
				return pinEntry{}, nil, fmt.Errorf("invalid initialization: %v", err)
			}
		case key == "resolution" && direction == "inputs":
			paired, err = replyFrom(value, name)
			if err != nil {
				return pinEntry{}, nil, err
			}
		default:
			return pinEntry{}, nil, fmt.Errorf("unknown %s pin key %q", direction, key)
		}
	}
	if initialize != nil && paired != nil {
		return pinEntry{}, nil, fmt.Errorf("unexpected combined reply/initialization")
	}
	return pinEntry{Event: name, Initialize: initialize}, paired, nil
}

func exactText(value yamlsource.Value) (string, error) {
	scalar, err := value.Scalar()
	if err != nil || scalar.Tag != "!!str" || scalar.Value == "" || scalar.Value != strings.TrimSpace(scalar.Value) || scalar.Anchor != "" || scalar.Alias != "" {
		return "", fmt.Errorf("expected exact non-empty text at %s", value.Location())
	}
	return scalar.Value, nil
}

func validEvent(value yamlsource.Value) (string, error) {
	text, err := exactText(value)
	if err != nil || !eventidentity.IsCanonicalName(text) || strings.Contains(text, "/") {
		return "", fmt.Errorf("invalid event at %s", value.Location())
	}
	return text, nil
}

func textMapping(value yamlsource.Value) (map[string]string, error) {
	fields, err := value.Mapping()
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, field := range fields {
		text, err := exactText(field.Value)
		if err != nil {
			return nil, err
		}
		out[field.Name] = text
	}
	return out, nil
}

func replyFrom(resolution yamlsource.Value, event string) (*reply, error) {
	fields, err := textMapping(resolution)
	if err != nil || fields["mode"] != "reply" {
		return nil, fmt.Errorf("unexpected pin resolution; only the ratified reply row is rewritten")
	}
	for key := range fields {
		if key != "mode" && key != "replies_to" && key != "correlation_key" {
			return nil, fmt.Errorf("unknown reply key %q", key)
		}
	}
	request, _ := lookupValue(resolution, "replies_to")
	requestName, err := validEvent(request)
	if err != nil {
		return nil, err
	}
	return &reply{event: event, request: requestName, correlation: fields["correlation_key"]}, nil
}

func moveReply(parent *source, item reply) error {
	if _, alreadyEdited := parent.edit["connect"]; alreadyEdited {
		return fmt.Errorf("multiple reply moves lie outside the ratified single-row corpus")
	}
	value, err := lookupValue(parent.root, "connect")
	if err != nil {
		return err
	}
	items, err := value.Sequence()
	if err != nil {
		return fmt.Errorf("missing exact reply connections")
	}
	connections := make([]map[string]string, 0, len(items))
	for _, row := range items {
		fields, err := textMapping(row)
		if err != nil {
			return err
		}
		connections = append(connections, fields)
	}
	response, err := connection(connections, "", "requester", item.event)
	if err != nil {
		return err
	}
	provider := response["from"]
	if provider == "" {
		return fmt.Errorf("missing provider identity")
	}
	if _, err := connection(connections, "requester", provider, item.request); err != nil {
		return err
	}
	if response["replies_to"] != "" {
		return fmt.Errorf("reply already has two declaration owners")
	}
	response["replies_to"] = item.request
	if item.correlation != "" {
		if response["correlation_key"] != "" {
			return fmt.Errorf("correlation already has two declaration owners")
		}
		response["correlation_key"] = item.correlation
	}
	parent.edit["connect"] = connections
	return nil
}

func connection(connections []map[string]string, from, to, event string) (map[string]string, error) {
	var matches []map[string]string
	for _, row := range connections {
		producer, receiver, name := row["from"], row["to"], row["event"]
		if receiver == "" || producer == "" || name == "" {
			return nil, fmt.Errorf("incomplete connection")
		}
		if renamed := row["rename"]; renamed != "" && from == "" {
			name = renamed
		}
		if receiver == to && name == event && (from == "" || producer == from) {
			matches = append(matches, row)
		}
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("expected one exact %s -> %s connection for %s, got %d", from, to, event, len(matches))
	}
	return matches[0], nil
}

func render(doc *source) ([]byte, error) {
	fields, err := doc.root.Mapping()
	if err != nil {
		return nil, err
	}
	lines := bytes.SplitAfter(doc.data, []byte("\n"))
	var output bytes.Buffer
	cursor := 0
	for i, field := range fields {
		value := doc.edit[field.Name]
		if value == nil {
			continue
		}
		start, end := field.KeyLocation.Line-1, len(lines)
		if i+1 < len(fields) {
			end = precedingCommentStart(lines, start, fields[i+1].KeyLocation.Line-1)
		}
		if field.KeyLocation.Column != 1 || start < cursor || end <= start {
			return nil, fmt.Errorf("unsupported inline root shape")
		}
		tail := end
		for tail > start+1 && strings.TrimSpace(string(lines[tail-1])) == "" {
			tail--
		}
		output.Write(bytes.Join(lines[cursor:start], nil))
		encoder := yaml.NewEncoder(&output)
		encoder.SetIndent(2)
		if err := encoder.Encode(map[string]any{field.Name: value}); err != nil {
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
