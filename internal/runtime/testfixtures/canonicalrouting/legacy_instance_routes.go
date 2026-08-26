package canonicalrouting

import (
	"path/filepath"
	"strings"
	"testing"
)

type TemplateInstanceRouteMode uint8

const (
	TemplateInstanceRouteSelect TemplateInstanceRouteMode = iota + 1
	TemplateInstanceRouteCreate
	TemplateInstanceRouteSelectOrCreate
)

type TemplateInstanceSecondPin uint8

const (
	TemplateInstanceNoSecondPin TemplateInstanceSecondPin = iota
	TemplateInstanceSecondPinSameEvent
	TemplateInstanceSecondPinDistinctEvent
	TemplateInstanceSecondPinDuplicateEdge
)

type TemplateInstanceConsumer uint8

const (
	TemplateInstanceNodeConsumer TemplateInstanceConsumer = iota
	TemplateInstanceAgentConsumer
	TemplateInstanceNodeAndAgentConsumer
)

type TemplateInstanceRouteOptions struct {
	Mode          TemplateInstanceRouteMode
	RenamedSource bool
	SecondPin     TemplateInstanceSecondPin
	Consumer      TemplateInstanceConsumer
}

// CopyTemplateInstanceRoute derives a closed scalar-identity lifecycle matrix.
// Every route uses receiver-owned resolution and a same-named typed carry.
func CopyTemplateInstanceRoute(t testing.TB, opts TemplateInstanceRouteOptions) string {
	t.Helper()
	if opts.Mode == 0 {
		opts.Mode = TemplateInstanceRouteSelect
	}
	mode := ""
	switch opts.Mode {
	case TemplateInstanceRouteSelect:
		mode = "select"
	case TemplateInstanceRouteCreate:
		mode = "create"
	case TemplateInstanceRouteSelectOrCreate:
		mode = "select-or-create"
	default:
		t.Fatalf("unsupported template instance route mode %d", opts.Mode)
	}

	root := CopyExample(t, ParentConnect)
	producerField := "vertical_id"
	carrySource := "payload.vertical_id"
	if opts.RenamedSource {
		producerField = "source_vertical_id"
		carrySource = "payload.source_vertical_id"
	}

	secondConnect := ""
	secondPin := ""
	secondHandler := ""
	if opts.SecondPin == TemplateInstanceSecondPinDuplicateEdge {
		secondConnect = "  - event: deploy.done\n    from: producer\n    to: consumer\n"
	} else if opts.SecondPin != TemplateInstanceNoSecondPin {
		secondConnect = "  - event: deploy.done\n    from: producer\n    to: consumer\n"
		secondEvent := "deploy.done"
		if opts.SecondPin == TemplateInstanceSecondPinDistinctEvent {
			secondConnect += "    rename: deploy.audited\n    adapter: deploy_done_to_deploy_audited\n"
			secondEvent = "deploy.audited"
			secondHandler = "    " + secondEvent + ": {}\n"
		} else if opts.SecondPin != TemplateInstanceSecondPinSameEvent {
			t.Fatalf("unsupported template instance second pin %d", opts.SecondPin)
		}
		secondPin = "      - name: deploy_audited\n        event: " + secondEvent + "\n        resolution:\n          mode: " + mode + "\n        carries:\n          vertical_id:\n            from: " + carrySource + "\n            type: string\n"
	}

	writeClosedVariantFile(t, root, "package.yaml", `name: template-instance-route
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - event: deploy.done
    from: producer
    to: consumer
`+secondConnect)
	writeClosedVariantFile(t, root, "schema.yaml", "name: template-instance-route\n")
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
`, "deploy.done:\n  "+producerField+": string\n", "", "")

	consumerNodes := "consumer-node:\n  id: consumer-node-{instance_id}\n  execution_type: system_node\n  event_handlers:\n    deploy.done: {}\n" + secondHandler
	consumerAgents := ""
	if opts.Consumer == TemplateInstanceAgentConsumer || opts.Consumer == TemplateInstanceNodeAndAgentConsumer {
		subscriptions := "deploy.done"
		if opts.SecondPin == TemplateInstanceSecondPinDistinctEvent {
			subscriptions += ", deploy.audited"
		}
		if opts.Consumer == TemplateInstanceAgentConsumer {
			consumerNodes = ""
		}
		consumerAgents = "consumer-agent:\n  id: consumer-agent\n  model: regular\n  intent:\n    inline: Consume connected deployment events.\n  subscriptions: [" + subscriptions + "]\n"
	} else if opts.Consumer != TemplateInstanceNodeConsumer {
		t.Fatalf("unsupported template instance consumer %d", opts.Consumer)
	}
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: template
instance: vertical_id
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
        resolution:
          mode: `+mode+`
        carries:
          vertical_id:
            from: `+carrySource+`
            type: string
`+secondPin,
		"",
		"deployment:\n  vertical_id:\n    type: string\n",
		consumerNodes)
	if consumerAgents != "" {
		writeClosedVariantFile(t, root, "flows/consumer/agents.yaml", consumerAgents)
	}
	return root
}

func writeLegacyInstanceFlow(t testing.TB, root, id, schema, events, entities, nodes string) {
	t.Helper()
	base := filepath.ToSlash(filepath.Join("flows", id))
	writeClosedVariantFile(t, root, base+"/schema.yaml", schema)
	if strings.TrimSpace(events) != "" {
		writeClosedVariantFile(t, root, base+"/events.yaml", events)
	}
	if strings.TrimSpace(entities) != "" {
		writeClosedVariantFile(t, root, base+"/entities.yaml", entities)
	}
	if strings.TrimSpace(nodes) != "" {
		writeClosedVariantFile(t, root, base+"/nodes.yaml", nodes)
	}
}
