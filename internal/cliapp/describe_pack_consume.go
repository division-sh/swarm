package cliapp

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"gopkg.in/yaml.v3"
)

func packConsumptionFragments(provider string, component PackComponentDescription) ([]PackConsumptionFragment, error) {
	setup := "# Keep the selected provider directory " + component.Directory + "; no import replaces these authored bytes.\n"
	if component.Source == "embedded" {
		setup = "# Import into the receiving source directory before using these fragments:\n# swarm import " + provider + " .\n"
	}
	fragments := []PackConsumptionFragment{{Scope: component.Scope, File: "provider setup", Purpose: "selected provider provenance", YAML: setup}}
	if component.HasChannel {
		policy, err := consumptionYAML(map[string]any{"operator_account": &yaml.Node{
			Kind: yaml.ScalarNode, Tag: "!!str", Value: "REPLACE_OPERATOR_ACCOUNT", Style: yaml.DoubleQuotedStyle,
		}})
		if err != nil {
			return nil, err
		}
		fragments = append(fragments, PackConsumptionFragment{Scope: component.Scope, File: "policy.yaml", Purpose: "author-selected holder account",
			RequiresSubstitution: true, YAML: "# Set the exact account selected by your deployment; no account is inferred or linked here.\n" + policy})
	}
	events := consumptionEvents(component.Receive)
	var example PackEventDescription
	receiver := ""
	if len(events) == 0 {
		fragments = append(fragments, PackConsumptionFragment{Scope: component.Scope, File: "schema.yaml", Purpose: "author-selected input",
			RequiresSubstitution: true, YAML: "# No fixed normalized input is declared. Author the input event and payload contract before adding an activity.\n"})
	} else {
		receive, err := packReceiveConsumptionFragments(provider, component, events)
		if err != nil {
			return nil, err
		}
		fragments = append(fragments, receive...)
		example, receiver = events[0], provider+"-receiver-1"
	}
	for _, tool := range component.Tools {
		fragment, err := packToolActivityFragment(component.Scope, receiver, example, tool)
		if err != nil {
			return nil, err
		}
		fragments = append(fragments, fragment)
	}
	return fragments, nil
}

func packReceiveConsumptionFragments(provider string, component PackComponentDescription, events []PackEventDescription) ([]PackConsumptionFragment, error) {
	var fragments []PackConsumptionFragment
	var inputs, outputs []string
	var connections []map[string]any
	for index, event := range events {
		receiver := provider + "-receiver-" + strconv.Itoa(index+1)
		inputs, outputs = append(inputs, event.Event), append(outputs, event.Event)
		connections = append(connections, map[string]any{"event": event.Event, "from": ".", "to": receiver, "resolution": "select-or-create"})
		child := map[string]any{"stages": []any{}, "inputs": []string{event.Event}}
		if event.AuthorSubject.Type == "chat" {
			child["instance"] = event.AuthorSubject.Field
		}
		doc, err := consumptionYAML(child)
		if err != nil {
			return nil, err
		}
		comment := "# No declared chat author subject: add an author-selected instance key if this consumer must be partitioned.\n"
		if event.AuthorSubject.Type == "chat" {
			comment = "# One instance per " + event.AuthorSubject.Field + "; change this declared-chat default if your application needs another partition.\n"
		}
		fragments = append(fragments, PackConsumptionFragment{Scope: component.Scope, File: receiver + "/schema.yaml",
			Purpose: "receiving flow", RequiresSubstitution: event.AuthorSubject.Type != "chat", YAML: comment + doc})
		manifest, err := consumptionYAML(map[string]any{"version": "1.0.0", "platform_version": "*"})
		if err != nil {
			return nil, err
		}
		fragments = append(fragments, PackConsumptionFragment{Scope: component.Scope, File: receiver + "/manifest.yaml",
			Purpose: "receiving flow manifest; keep your project's version policy", YAML: manifest})
		if event.AuthorSubject.Type == "chat" {
			state, err := consumptionYAML(map[string]any{event.AuthorSubject.Field: map[string]any{"type": "text", "indexed": true}})
			if err != nil {
				return nil, err
			}
			fragments = append(fragments, PackConsumptionFragment{Scope: component.Scope, File: receiver + "/state.yaml",
				Purpose: "declared chat identity state", YAML: state})
		}
	}
	root, err := consumptionYAML(map[string]any{"inputs": inputs, "outputs": outputs, "connect": connections})
	if err != nil {
		return nil, err
	}
	transport := "# Ingress is derived from the provider directory held by this flow and its declared inputs.\n"
	if component.Receive.Transport == packs.ChannelTransportWebhook {
		transport += "# The webhook alias is derived from the admitted root/flow path; renaming that path changes the URL.\n# Current schema has no ingress/alias override.\n"
	}
	fragments = append(fragments, PackConsumptionFragment{Scope: component.Scope, File: "schema.yaml", Purpose: "receive/connect fragment; merge with the owning flow schema", YAML: transport + root})
	return fragments, nil
}

func consumptionEvents(receive *PackReceiveDescription) []PackEventDescription {
	if receive == nil {
		return nil
	}
	var normalized, raw []PackEventDescription
	for _, event := range receive.Events {
		if !runtimeidentity.IsCanonicalName(event.Event) {
			continue
		}
		if event.Kind == "normalized" {
			normalized = append(normalized, event)
		} else {
			raw = append(raw, event)
		}
	}
	if len(normalized) != 0 {
		return normalized
	}
	return raw
}

func packToolActivityFragment(scope, receiver string, event PackEventDescription, tool PackToolDescription) (PackConsumptionFragment, error) {
	input := map[string]any{}
	var notes []string
	for _, name := range tool.Input.RequiredProperties() {
		schema, _ := tool.Input.Property(name)
		input[name] = consumptionPlaceholder(name, schema)
		for _, field := range event.Fields {
			if field.Name == name && field.Required && consumptionFieldType(field.Type, schema.Kind()) {
				input[name] = "payload." + name
			}
		}
		notes = append(notes, fmt.Sprintf("# %s: required %s", name, schema.Kind()))
	}
	activity := map[string]any{"id": "consume_event", "tool": tool.ID, "input": input}
	var nodes any = map[string]any{"activity": activity}
	file, purpose := "nodes.yaml", "tool activity fragment: "+tool.ID
	placement := "# Insert this activity under your author-declared handler; no event or route is supplied.\n"
	if event.Event != "" {
		nodes = map[string]any{"consume": map[string]any{event.Event: map[string]any{"activity": activity}}}
		file, purpose = receiver+"/nodes.yaml", "tool example: "+tool.ID
		placement = ""
	}
	doc, err := consumptionYAML(nodes)
	if err != nil {
		return PackConsumptionFragment{}, err
	}
	comment := placement + "# Choose this tool example deliberately; do not combine every tool example.\n# Replace REPLACE_* values with your business inputs before execution.\n"
	if len(notes) > 0 {
		comment += strings.Join(notes, "\n") + "\n"
	}
	return PackConsumptionFragment{Scope: scope, File: file, Purpose: purpose,
		RequiresSubstitution: true, YAML: comment + doc}, nil
}

func consumptionPlaceholder(name string, schema runtimecontracts.ToolInputSchema) any {
	switch schema.Kind() {
	case runtimecontracts.ToolSchemaBoolean:
		return false
	case runtimecontracts.ToolSchemaInteger, runtimecontracts.ToolSchemaNumber:
		return 0
	case runtimecontracts.ToolSchemaArray:
		return []any{}
	case runtimecontracts.ToolSchemaObject:
		return map[string]any{}
	default:
		return strconv.Quote("REPLACE_" + strings.ToUpper(name))
	}
}

func consumptionFieldType(field string, kind runtimecontracts.ToolSchemaKind) bool {
	return field == string(kind) || field == "text" && kind == runtimecontracts.ToolSchemaString ||
		field == "int" && kind == runtimecontracts.ToolSchemaInteger || field == "bool" && kind == runtimecontracts.ToolSchemaBoolean
}

func consumptionYAML(value any) (string, error) {
	var document yaml.Node
	if err := document.Encode(value); err != nil {
		return "", err
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return "", err
	}
	if err := encoder.Close(); err != nil {
		return "", err
	}
	return out.String(), nil
}
