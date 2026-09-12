package canonicalrouting

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// CopyPublicationSites exercises real emission sites, with exact, wildcard and
// connected consumers. The sibling repeats all local names but has no edge.
func CopyPublicationSites(t testing.TB, mode string) string {
	t.Helper()
	return copyPublicationSites(t, mode, false)
}

// CopyPublicationTextSites isolates publication identity from #2402's separately
// retained deferred numeric-value projection regression.
func CopyPublicationTextSites(t testing.TB, mode string) string {
	t.Helper()
	return copyPublicationSites(t, mode, true)
}

func copyPublicationSites(t testing.TB, mode string, textValues bool) string {
	t.Helper()
	if mode != "root" && mode != "static" && mode != "template" {
		t.Fatalf("unsupported publication topology %q", mode)
	}
	root := t.TempDir()
	valueType, valueExpr := "integer", "payload.choice"
	if textValues {
		valueType, valueExpr = "text", "string(payload.choice)"
	}
	families := []string{"direct", "rules", "specialized", "completion", "success", "fanout", "rulefanout", "completefanout"}
	scope := "source"
	if mode == "root" {
		scope = "."
	}
	connects, inputPins, outputPins, eventSchemas, handlers := "", "", "", "", ""
	for _, family := range families {
		request, result := family+".requested", "result."+family
		inputPins += fmt.Sprintf("      - {event: %s, source: external}\n", request)
		outputPins += "      - " + result + "\n"
		connects += fmt.Sprintf("  - {event: %s, from: %s, to: sink}\n", result, scope)
		eventSchemas += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  choice: integer\n  items: '[%s]'\n%s:\n  case_id: text\n  value: %s\n", request, valueType, result, valueType)
		emit := fmt.Sprintf("{event: %s, fields: {case_id: payload.case_id, value: %s}}", result, valueExpr)
		body := "      emit: " + emit + "\n"
		switch family {
		case "rules", "specialized", "completion":
			placement := "rules"
			if family == "completion" {
				placement = "on_complete"
			}
			body = ""
			if family == "specialized" {
				body = fmt.Sprintf("      emit: {event: %s, fields: {case_id: payload.case_id}}\n", result)
			}
			body += "      " + placement + ":\n"
			for _, choice := range []struct {
				name, condition string
				value           int
			}{{"positive", "payload.choice > 0", 10}, {"otherwise", "else", 20}} {
				literal := strconv.Itoa(choice.value)
				if textValues {
					literal = strconv.Quote(literal)
				}
				selected := fmt.Sprintf("{event: %s, fields: {case_id: payload.case_id, value: {literal: %s}}}", result, literal)
				if family == "specialized" {
					selected = fmt.Sprintf("{fields: {value: '%s'}}", literal)
				}
				body += fmt.Sprintf("        - id: %s\n          condition: '%s'\n          emit: %s\n", choice.name, choice.condition, selected)
			}
		case "success":
			body = "      rules:\n        - {id: selected, condition: else}\n      on_success:\n        emit: " + emit + "\n"
		case "fanout", "rulefanout", "completefanout":
			indent := "      "
			body = ""
			if family != "fanout" {
				placement := "rules"
				if family == "completefanout" {
					placement = "on_complete"
				}
				body = "      " + placement + ":\n        - id: dispatch\n          condition: else\n"
				indent = "          "
			}
			body += indent + "fan_out:\n" + indent + "  items_from: payload.items\n" + indent + "  as: element\n" + indent + "  identity: element\n" + indent + "  emit: " + strings.Replace(emit, "value: "+valueExpr, "value: element", 1) + "\n"
		}
		handlers += fmt.Sprintf("%s:\n  execution_type: system_node\n  subscribes_to: [%s]\n  event_handlers:\n    %s:\n%s", family, request, request, body)
	}
	results := make([]string, 0, len(families))
	for _, family := range families {
		results = append(results, "result."+family)
	}
	local := "local:\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(results, ", ") + "]\n  event_handlers:\n"
	for _, result := range results {
		local += "    " + result + ":\n      guard: {id: observed, check: 'int(payload.value) >= 0'}\n"
	}
	wildcard := "wildcard:\n  execution_type: system_node\n  subscribes_to: [result.*]\n  event_handlers:\n    result.*:\n      guard: {id: observed, check: 'int(payload.value) >= 0'}\n"
	sourceSchema := "name: publication-source\npins:\n  inputs:\n    events:\n" + inputPins + "  outputs:\n    events:\n" + outputPins
	if mode == "root" {
		writeClosedVariantFile(t, root, "schema.yaml", sourceSchema+"connect:\n"+connects)
		writeClosedVariantFile(t, root, "events.yaml", eventSchemas)
		writeClosedVariantFile(t, root, "nodes.yaml", handlers+local+wildcard)
	} else {
		rootSchema := "name: publication-driver\n"
		if mode == "template" {
			sourceSchema = strings.Replace(sourceSchema, "name: publication-source\n", "name: publication-source\nmode: template\ninstance: case_id\n", 1)
			sourceSchema = strings.ReplaceAll(sourceSchema, "source: external", "resolution: {mode: select-or-create}")
			rootSchema += "pins:\n  inputs:\n    events:\n" + inputPins + "  outputs:\n    events:\n"
			driver, driverSchemas := "", ""
			for _, family := range families {
				request, dispatch := family+".requested", family+".dispatch"
				rootSchema += "      - " + dispatch + "\n"
				connects += fmt.Sprintf("  - {event: %s, from: ., to: source, rename: %s}\n", dispatch, request)
				driverSchemas += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  choice: integer\n  items: '[%s]'\n", request, valueType)
				driverSchemas += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  choice: integer\n  items: '[%s]'\n", dispatch, valueType)
				driver += fmt.Sprintf("%s:\n  execution_type: system_node\n  subscribes_to: [%s]\n  event_handlers:\n    %s:\n      emit: {event: %s, fields: {case_id: payload.case_id, choice: payload.choice, items: payload.items}}\n", family, request, request, dispatch)
			}
			writeClosedVariantFile(t, root, "nodes.yaml", driver)
			writeClosedVariantFile(t, root, "events.yaml", driverSchemas)
		}
		writeClosedVariantFile(t, root, "schema.yaml", rootSchema+"connect:\n"+connects)
		writeClosedVariantFile(t, root, "source/schema.yaml", sourceSchema)
		sourceEvents := eventSchemas
		if mode == "template" {
			sourceEvents = ""
			for _, family := range families {
				sourceEvents += "result." + family + ":\n  case_id: text\n  value: " + valueType + "\n"
			}
		}
		writeClosedVariantFile(t, root, "source/events.yaml", sourceEvents)
		writeClosedVariantFile(t, root, "source/nodes.yaml", handlers+local+wildcard)
		if mode == "template" {
			writeClosedVariantFile(t, root, "source/entities.yaml", "work:\n  case_id: {type: text, _unused_reason: receiver identity}\n")
		}
	}
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    events:\n"+outputPins)
	writeClosedVariantFile(t, root, "sink/nodes.yaml", local)
	// A sibling's local subscriptions are not authority for source publications.
	siblingInputs, siblingEvents := "", ""
	for _, result := range results {
		siblingInputs += "      - {event: " + result + ", source: external}\n"
		siblingEvents += result + ":\n  case_id: text\n  value: " + valueType + "\n"
	}
	writeClosedVariantFile(t, root, "sibling/schema.yaml", "name: sibling\npins:\n  inputs:\n    events:\n"+siblingInputs)
	writeClosedVariantFile(t, root, "sibling/events.yaml", siblingEvents)
	writeClosedVariantFile(t, root, "sibling/nodes.yaml", local+wildcard)
	return root
}
