package specaudit

import (
	"os"
	"testing"

	"empireai/internal/commgraph"
)

func TestMain(m *testing.M) {
	commgraph.SetDefaultPolicyFactory(func() commgraph.Policy {
		return commgraph.NewGenericTestPolicy()
	})
	os.Exit(m.Run())
}
