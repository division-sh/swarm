package main

import (
	"empireai/internal/commgraph"
	commgraphempire "empireai/internal/commgraph/empire"
)

func ensureEmpireCommgraphPolicy() {
	commgraph.SetDefaultPolicyFactory(func() commgraph.Policy {
		return commgraphempire.New()
	})
}
