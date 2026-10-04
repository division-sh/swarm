package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyRootReplyBoundary fixes the topology for the two root-reply intersections.
// Only external lifecycle inputs and the committed result are public.
func CopyRootReplyBoundary(t testing.TB, rootRequester, explicitCorrelation bool) string {
	t.Helper()
	root := CopyExample(t, RootIngress)
	removeInheritedScenarios(t, root)
	writeClosedVariantFile(t, root, "entities.yaml", "request_state: {}\n")
	requester, provider := ".", "provider"
	if !rootRequester {
		requester, provider = "requester", "."
	}
	correlation := ""
	if explicitCorrelation {
		correlation = "    correlation_key: request_id\n"
	}
	connect := "  - {event: request.started, from: ., to: requester}\n"
	if rootRequester {
		connect = ""
	}
	connect += "  - {event: provider.requested, from: " + requester + ", to: " + provider + "}\n"
	connect += "  - event: provider.replied\n    from: " + provider + "\n    to: " + requester + "\n    replies_to: provider.requested\n" + correlation
	if !rootRequester {
		connect += "  - {event: request.finished, from: requester, to: .}\n"
	}
	writeClosedVariantFile(t, root, "schema.yaml", "name: root-reply-boundary\nstages:\n  waiting: {initial: true}\n  done: {terminal: true}\npins:\n  inputs: [request.started, request.stop]\n  outputs: [request.finished]\nconnect:\n"+connect)
	startSchema := "request.started:\n  account_id: text\n  request_id: text\nrequest.stop:\n"
	requestSchema := "provider.requested:\n  account_id: text\n  request_id: text\n"
	responseSchema := "provider.replied:\n  account_id: text\n  request_id: text\n  result: text\n"
	finishedSchema := "request.finished:\n  request_id: text\n  result: text\n"
	requesterNode := `requester-node:
  execution_type: system_node
  subscribes_to: [request.started, provider.replied]
  event_handlers:
    request.started:
      emit:
        event: provider.requested
        fields:
          account_id: ${payload.account_id}
          request_id: ${payload.request_id}
    provider.replied:
      emit:
        event: request.finished
        fields:
          request_id: ${payload.request_id}
          result: ${payload.result}
`
	providerNode := `provider-node:
  execution_type: system_node
  subscribes_to: [provider.requested]
  event_handlers:
    provider.requested:
      emit:
        event: provider.replied
        fields:
          account_id: ${payload.account_id}
          request_id: ${payload.request_id}
          result: {literal: approved}
`
	stopNode := `stop-node:
  execution_type: system_node
  subscribes_to: [request.stop]
  event_handlers:
    request.stop: {advances_to: done}
`
	if rootRequester {
		writeClosedVariantFile(t, root, "events.yaml", startSchema+requestSchema+finishedSchema)
		writeClosedVariantFile(t, root, "nodes.yaml", stopNode+requesterNode+`result-observer:
  execution_type: system_node
  subscribes_to: [request.finished]
  event_handlers:
    request.finished: {}
`)
		writeClosedVariantFile(t, root, "provider/schema.yaml", "name: provider\npins:\n  inputs: [provider.requested]\n  outputs: [provider.replied]\n")
		writeClosedVariantFile(t, root, "provider/events.yaml", responseSchema)
		writeClosedVariantFile(t, root, "provider/nodes.yaml", providerNode)
	} else {
		writeClosedVariantFile(t, root, "events.yaml", startSchema+responseSchema)
		writeClosedVariantFile(t, root, "nodes.yaml", stopNode+providerNode+`result-observer:
  execution_type: system_node
  subscribes_to: [request.finished]
  event_handlers:
    request.finished: {}
`)
		writeClosedVariantFile(t, root, "requester/schema.yaml", "name: requester\npins:\n  inputs: [request.started, provider.replied]\n  outputs: [provider.requested, request.finished]\n")
		writeClosedVariantFile(t, root, "requester/events.yaml", requestSchema+finishedSchema)
		writeClosedVariantFile(t, root, "requester/nodes.yaml", requesterNode)
	}
	return root
}

type RootReplyNegativeMutation uint8

const (
	RootReplyCorrelationAbsent RootReplyNegativeMutation = iota + 1
	RootReplyCorrelationOptional
	RootReplyCorrelationList
	RootReplyCorrelationIncompatible
	RootReplyLocalConsumerAbsent
	RootReplyExportObserverAbsent
)

func ApplyRootReplyBoundaryNegativeMutation(t testing.TB, root string, rootRequester bool, mutation RootReplyNegativeMutation) {
	t.Helper()
	if mutation == RootReplyLocalConsumerAbsent || mutation == RootReplyExportObserverAbsent {
		path := filepath.Join(root, "nodes.yaml")
		doc := readYAMLDocument(t, path)
		node := "provider-node"
		if rootRequester {
			node = "requester-node"
		}
		if mutation == RootReplyExportObserverAbsent {
			node = "result-observer"
		}
		if requireYAMLMapping(t, path, doc.Content[0])[node] == nil {
			t.Fatalf("missing root reply consumer %s", node)
		}
		for i := 0; i < len(doc.Content[0].Content); i += 2 {
			if doc.Content[0].Content[i].Value == node {
				doc.Content[0].Content = append(doc.Content[0].Content[:i], doc.Content[0].Content[i+2:]...)
				break
			}
		}
		writeYAMLDocument(t, path, doc)
		return
	}
	path := filepath.Join(root, "events.yaml")
	if rootRequester {
		path = filepath.Join(root, "provider", "events.yaml")
	}
	field := ""
	switch mutation {
	case RootReplyCorrelationAbsent:
	case RootReplyCorrelationOptional:
		field = "  request_id: text?\n"
	case RootReplyCorrelationList:
		field = "  request_id: \"[text]\"\n"
	case RootReplyCorrelationIncompatible:
		field = "  request_id: integer\n"
	default:
		t.Fatalf("unknown root reply negative mutation %d", mutation)
	}
	applyClosedReplacement(t, path, "provider.replied:\n  account_id: text\n  request_id: text\n", "provider.replied:\n  account_id: text\n"+field)
}

func RenameRootReplyEndpoints(t testing.TB, root string, rootRequester bool) {
	t.Helper()
	request := "  - {event: provider.requested, from: ., to: provider}\n"
	replacement := "  - {event: provider.requested, from: ., to: provider, rename: provider.received}\n"
	nodes := filepath.Join(root, "provider", "nodes.yaml")
	if !rootRequester {
		request = "  - {event: provider.requested, from: requester, to: .}\n"
		replacement = "  - {event: provider.requested, from: requester, to: ., rename: provider.received}\n"
		nodes = filepath.Join(root, "nodes.yaml")
	} else {
		applyClosedReplacement(t, filepath.Join(root, "provider", "schema.yaml"), "inputs: [provider.requested]", "inputs: [provider.received]")
	}
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), request, replacement)
	applyClosedReplacement(t, nodes, "subscribes_to: [provider.requested]", "subscribes_to: [provider.received]")
	applyClosedReplacement(t, nodes, "    provider.requested:\n", "    provider.received:\n")
	applyClosedReplacement(t, filepath.Join(root, "schema.yaml"), "  - event: provider.replied\n", "  - event: provider.replied\n    rename: requester.received\n")
	nodes = filepath.Join(root, "nodes.yaml")
	if !rootRequester {
		nodes = filepath.Join(root, "requester", "nodes.yaml")
		applyClosedReplacement(t, filepath.Join(root, "requester", "schema.yaml"), "inputs: [request.started, provider.replied]", "inputs: [request.started, requester.received]")
	}
	applyClosedReplacement(t, nodes, "subscribes_to: [request.started, provider.replied]", "subscribes_to: [request.started, requester.received]")
	applyClosedReplacement(t, nodes, "    provider.replied:\n", "    requester.received:\n")
}

func CopyRootReplyConsumerBoundary(t testing.TB, rootRequester, reply, nested, agent bool) (root, owner string) {
	t.Helper()
	owner = CopyRootReplyBoundary(t, rootRequester, false)
	if !reply {
		applyClosedReplacement(t, filepath.Join(owner, "schema.yaml"), "    replies_to: provider.requested\n", "")
	}
	if agent {
		ApplyRootReplyBoundaryNegativeMutation(t, owner, rootRequester, RootReplyLocalConsumerAbsent)
		event := "provider.requested"
		if rootRequester {
			event = "provider.replied"
		}
		writeClosedVariantFile(t, owner, "agents.yaml", "local-consumer:\n  intent: {inline: Observe the connected event.}\n  model: regular\n  subscriptions: ["+event+"]\n")
	}
	root = owner
	if nested {
		root = t.TempDir()
		copyTree(t, owner, filepath.Join(root, "branch"))
		owner = filepath.Join(root, "branch")
		writeClosedVariantFile(t, root, "schema.yaml", "name: nested-reply-boundary\n")
	}
	return root, owner
}

func RemoveRootReplyConsumer(t testing.TB, owner string, rootRequester, agent bool) func() {
	t.Helper()
	path := filepath.Join(owner, "nodes.yaml")
	if agent {
		path = filepath.Join(owner, "agents.yaml")
	}
	original := readYAMLDocument(t, path)
	if agent {
		removeClosedVariantFiles(t, owner, "agents.yaml")
	} else {
		ApplyRootReplyBoundaryNegativeMutation(t, owner, rootRequester, RootReplyLocalConsumerAbsent)
	}
	return func() { writeYAMLDocument(t, path, original) }
}
