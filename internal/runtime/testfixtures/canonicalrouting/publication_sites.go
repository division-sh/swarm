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

func CopyPublicationDirectTextSite(t testing.TB, mode string) string {
	t.Helper()
	return copyPublicationSites(t, mode, true, "direct")
}

func copyPublicationSites(t testing.TB, mode string, textValues bool, selectedFamilies ...string) string {
	t.Helper()
	if mode != "root" && mode != "static" && mode != "template" {
		t.Fatalf("unsupported publication topology %q", mode)
	}
	root := t.TempDir()
	valueType, valueExpr := "integer", `"${payload.choice}"`
	if textValues {
		valueType, valueExpr = "text", `"${string(payload.choice)}"`
	}
	families := []string{"direct", "rules", "specialized", "completion", "success", "fanout", "rulefanout", "completefanout"}
	if len(selectedFamilies) != 0 {
		families = selectedFamilies
	}
	scope := "source"
	if mode == "root" {
		scope = "."
	}
	connects, inputPins, outputPins, eventSchemas, requestSchemas, handlers := "", "", "", "", "", ""
	siblingInputPins, siblingRequestSchemas, siblingProducers := "", "", ""
	for _, family := range families {
		request, result := family+".requested", "result."+family
		siblingRequest := "sibling." + family + ".requested"
		siblingInputPins += "      - " + siblingRequest + "\n"
		siblingRequestSchemas += fmt.Sprintf("%s:\n  case_id: text\n  value: %s\n", siblingRequest, valueType)
		siblingProducers += fmt.Sprintf("sibling-%s:\n  execution_type: system_node\n  subscribes_to: [%s]\n  event_handlers:\n    %s:\n      emit: {event: %s, fields: {case_id: \"${payload.case_id}\", value: \"${payload.value}\"}}\n", family, siblingRequest, siblingRequest, result)
		connects += fmt.Sprintf("  - {event: %s, from: ., to: sibling}\n", siblingRequest)
		inputPins += "      - " + request + "\n"
		outputPins += "      - " + result + "\n"
		connects += fmt.Sprintf("  - {event: %s, from: %s, to: sink}\n", result, scope)
		requestSchemas += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  choice: integer\n  items: '[%s]'\n", request, valueType)
		eventSchemas += fmt.Sprintf("%s:\n  case_id: text\n  value: %s\n", result, valueType)
		emit := fmt.Sprintf("{event: %s, fields: {case_id: \"${payload.case_id}\", value: %s}}", result, valueExpr)
		body := "      emit: " + emit + "\n"
		switch family {
		case "rules", "specialized", "completion":
			placement := "rules"
			if family == "completion" {
				placement = "on_complete"
			}
			body = ""
			if family == "specialized" {
				body = fmt.Sprintf("      emit: {event: %s, fields: {case_id: \"${payload.case_id}\"}}\n", result)
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
				selected := fmt.Sprintf("{event: %s, fields: {case_id: \"${payload.case_id}\", value: {literal: %s}}}", result, literal)
				if family == "specialized" {
					selected = fmt.Sprintf("{fields: {value: {literal: %s}}}", literal)
				}
				predicate := fmt.Sprintf("condition: '%s'", choice.condition)
				if placement == "rules" {
					predicate = fmt.Sprintf("when: '%s'", choice.condition)
					if choice.condition == "else" {
						predicate = "else: true"
					}
				}
				body += fmt.Sprintf("        - id: %s\n          %s\n          emit: %s\n", choice.name, predicate, selected)
			}
		case "success":
			body = "      rules:\n        - {id: selected, else: true}\n      on_success:\n        emit: " + emit + "\n"
		case "fanout", "rulefanout", "completefanout":
			indent := "      "
			body = ""
			if family != "fanout" {
				placement := "rules"
				if family == "completefanout" {
					placement = "on_complete"
				}
				predicate := "condition: else"
				if placement == "rules" {
					predicate = "else: true"
				}
				body = "      " + placement + ":\n        - id: dispatch\n          " + predicate + "\n"
				indent = "          "
			}
			body += indent + "fan_out:\n" + indent + "  items_from: payload.items\n" + indent + "  as: element\n" + indent + "  identity: element\n" + indent + "  emit: " + strings.Replace(emit, "value: "+valueExpr, `value: "${element}"`, 1) + "\n"
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
		sourceSchema = strings.Replace(sourceSchema, "  outputs:\n    events:\n", siblingInputPins+"  outputs:\n    events:\n"+siblingInputPins, 1)
		writeClosedVariantFile(t, root, "schema.yaml", sourceSchema+"connect:\n"+connects)
		writeClosedVariantFile(t, root, "events.yaml", requestSchemas+eventSchemas+siblingRequestSchemas)
		writeClosedVariantFile(t, root, "nodes.yaml", handlers+local+wildcard)
	} else {
		rootSchema := "name: publication-driver\npins:\n  inputs:\n    events:\n"
		if mode == "template" {
			sourceSchema = strings.Replace(sourceSchema, "name: publication-source\n", "name: publication-source\ninstance: case_id\n", 1)
			rootSchema += requestSchemasToPins(families) + "  outputs:\n    events:\n"
			driver, driverSchemas := "", ""
			for _, family := range families {
				request, dispatch := family+".requested", family+".dispatch"
				rootSchema += "      - " + dispatch + "\n"
				connects += fmt.Sprintf("  - {event: %s, from: ., to: source, rename: %s, resolution: select-or-create}\n", dispatch, request)
				driverSchemas += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  choice: integer\n  items: '[%s]'\n", request, valueType)
				driverSchemas += fmt.Sprintf("%s:\n  key: case_id\n  case_id: text\n  choice: integer\n  items: '[%s]'\n", dispatch, valueType)
				driver += fmt.Sprintf("%s:\n  execution_type: system_node\n  subscribes_to: [%s]\n  event_handlers:\n    %s:\n      emit: {event: %s, fields: {case_id: \"${payload.case_id}\", choice: \"${payload.choice}\", items: \"${payload.items}\"}}\n", family, request, request, dispatch)
			}
			writeClosedVariantFile(t, root, "nodes.yaml", driver)
			writeClosedVariantFile(t, root, "events.yaml", driverSchemas+siblingRequestSchemas)
		} else {
			rootSchema += inputPins + "  outputs:\n    events:\n" + requestSchemasToPins(families)
			for _, family := range families {
				request := family + ".requested"
				connects += fmt.Sprintf("  - {event: %s, from: ., to: source}\n", request)
			}
			writeClosedVariantFile(t, root, "events.yaml", requestSchemas+siblingRequestSchemas)
		}
		rootSchema = strings.Replace(rootSchema, "  outputs:\n    events:\n", siblingInputPins+"  outputs:\n    events:\n"+siblingInputPins, 1)
		writeClosedVariantFile(t, root, "schema.yaml", rootSchema+"connect:\n"+connects)
		writeClosedVariantFile(t, root, "source/schema.yaml", sourceSchema)
		writeClosedVariantFile(t, root, "source/events.yaml", eventSchemas)
		writeClosedVariantFile(t, root, "source/nodes.yaml", handlers+local+wildcard)
		if mode == "template" {
			writeClosedVariantFile(t, root, "source/entities.yaml", "work:\n  case_id: {type: text, _unused_reason: receiver identity}\n")
		}
	}
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    events:\n"+outputPins)
	writeClosedVariantFile(t, root, "sink/nodes.yaml", local)
	// A sibling's local subscriptions are not authority for source publications.
	siblingEvents := ""
	for _, result := range results {
		siblingEvents += result + ":\n  case_id: text\n  value: " + valueType + "\n"
	}
	writeClosedVariantFile(t, root, "sibling/schema.yaml", "name: sibling\npins:\n  inputs:\n    events:\n"+siblingInputPins)
	writeClosedVariantFile(t, root, "sibling/events.yaml", siblingEvents)
	writeClosedVariantFile(t, root, "sibling/nodes.yaml", siblingProducers+local)
	return root
}

func requestSchemasToPins(families []string) string {
	var pins string
	for _, family := range families {
		pins += "      - " + family + ".requested\n"
	}
	return pins
}
