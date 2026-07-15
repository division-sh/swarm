package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// LegacyInstancePolicy is the closed policy vocabulary still exercised by the
// #1738 retirement tests. It cannot introduce arbitrary authoring syntax.
type LegacyInstancePolicy uint8

const (
	LegacyInstancePolicyDefault LegacyInstancePolicy = iota
	LegacyInstancePolicyReject
	LegacyInstancePolicyCreate
	LegacyInstancePolicyReuse
)

type LegacyInstanceAdapter uint8

const (
	LegacyInstanceAdapterIdentity LegacyInstanceAdapter = iota
	LegacyInstanceAdapterRenamed
	LegacyInstanceAdapterInvalidTarget
)

type LegacyInstanceRouteOptions struct {
	Missing  LegacyInstancePolicy
	Conflict LegacyInstancePolicy
	Adapter  LegacyInstanceAdapter
	MultiPin bool
}

// CopyLegacyInstanceRoute derives the closed #1738 instance-policy matrix from
// the checked-in parent-connect artifact. Callers choose typed legacy states;
// they cannot supply route-bearing YAML.
func CopyLegacyInstanceRoute(t testing.TB, opts LegacyInstanceRouteOptions) string {
	t.Helper()
	root := CopyExample(t, ParentConnect)
	missing := legacyInstancePolicy(t, opts.Missing)
	conflict := legacyInstancePolicy(t, opts.Conflict)

	producerKey := "vertical_id"
	targetKey := "vertical_id"
	using := ""
	if opts.Adapter != LegacyInstanceAdapterIdentity {
		producerKey = "source_vertical_id"
		usingTarget := targetKey
		if opts.Adapter == LegacyInstanceAdapterInvalidTarget {
			usingTarget = "missing_vertical_id"
		} else if opts.Adapter != LegacyInstanceAdapterRenamed {
			t.Fatalf("unsupported legacy instance adapter %d", opts.Adapter)
		}
		using = "    using:\n      instance:\n        source: source_vertical_id\n        target: " + usingTarget + "\n"
	}
	secondConnect := ""
	secondPin := ""
	if opts.MultiPin {
		secondConnect = "  - from: producer.deploy_done\n    to: consumer.deploy_audited\n"
		secondPin = "      - name: deploy_audited\n        event: deploy.done\n"
	}

	writeClosedVariantFile(t, root, "package.yaml", `name: legacy-instance-route
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
  - from: producer.deploy_done
    to: consumer.deploy_completed
`+using+secondConnect)
	writeClosedVariantFile(t, root, "schema.yaml", "name: legacy-instance-route\n")
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml", "events.yaml", "nodes.yaml"} {
		writeClosedVariantFile(t, root, file, "{}\n")
	}
	writeLegacyInstanceFlow(t, root, "producer", `name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: `+producerKey+`
        carries: [`+producerKey+`]
`, "deploy.done:\n  "+producerKey+": string\n", "{}\n", "{}\n")

	policy := ""
	if missing != "" {
		policy += "  on_missing: " + missing + "\n"
	}
	if conflict != "" {
		policy += "  on_conflict: " + conflict + "\n"
	}
	writeLegacyInstanceFlow(t, root, "consumer", `name: consumer
mode: template
instance:
  by: `+targetKey+`
`+policy+`pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`+secondPin,
		"deploy.done:\n  "+producerKey+": string\n",
		"deployment:\n  vertical_id:\n    type: string\n",
		"consumer-node:\n  id: consumer-node-{instance_id}\n  execution_type: system_node\n  event_handlers:\n    deploy.done: {}\n")
	return root
}

func legacyInstancePolicy(t testing.TB, value LegacyInstancePolicy) string {
	t.Helper()
	switch value {
	case LegacyInstancePolicyDefault:
		return ""
	case LegacyInstancePolicyReject:
		return "reject"
	case LegacyInstancePolicyCreate:
		return "create"
	case LegacyInstancePolicyReuse:
		return "reuse"
	default:
		t.Fatalf("unsupported legacy instance policy %d", value)
		return ""
	}
}

func writeLegacyInstanceFlow(t testing.TB, root, id, schema, events, entities, nodes string) {
	t.Helper()
	base := filepath.ToSlash(filepath.Join("flows", id))
	writeClosedVariantFile(t, root, base+"/schema.yaml", schema)
	writeClosedVariantFile(t, root, base+"/events.yaml", events)
	writeClosedVariantFile(t, root, base+"/entities.yaml", entities)
	writeClosedVariantFile(t, root, base+"/nodes.yaml", nodes)
	for _, file := range []string{"policy.yaml", "tools.yaml", "agents.yaml"} {
		writeClosedVariantFile(t, root, base+"/"+file, "{}\n")
	}
}
