package specaudit

import (
	"empireai/internal/commgraph"
	commgraphempire "empireai/internal/commgraph/empire"
)

func init() {
	commgraph.SetDefaultPolicyFactory(func() commgraph.Policy {
		return commgraphempire.New()
	})
}
