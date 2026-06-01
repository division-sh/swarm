package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func checkGateSchemaValidation(c *checkerContext) []Finding { return c.gateSchemaValidation() }

func (c *checkerContext) gateSchemaValidation() []Finding {
	if c.gateSchemaLoaded {
		return c.gateSchemaFindings
	}
	c.gateSchemaLoaded = true
	nodes := c.source.NodeEntries()
	for _, transition := range c.source.DerivedHandlerTransitions() {
		gate := gateNameLocal(transition.SetsGate)
		if gate == "" {
			continue
		}
		node, ok := nodes[strings.TrimSpace(transition.NodeID)]
		if !ok {
			continue
		}
		validGates := stateSchemaGateNamesLocal(node.GateState)
		if _, ok := validGates[gate]; ok {
			continue
		}
		c.gateSchemaFindings = append(c.gateSchemaFindings, Finding{
			CheckID:  "gate_schema_validation",
			Severity: "error",
			Message:  fmt.Sprintf("handler transition %s sets_gate %s not recognized in node %s gate_state schema", transition.ID, gate, transition.NodeID),
			Location: strings.TrimSpace(transition.ID),
		})
	}
	return c.gateSchemaFindings
}

func stateSchemaGateNamesLocal(schema runtimecontracts.NodeGateStateSchema) map[string]struct{} {
	gates := map[string]struct{}{}
	for _, f := range schema.Gates {
		if strings.TrimSpace(f.Name) != "" {
			gates[strings.TrimSpace(f.Name)] = struct{}{}
		}
	}
	return gates
}
