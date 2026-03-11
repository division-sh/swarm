package pipeline

import (
	"flag"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()
	SetDefaultWorkflowModuleFactory(func() WorkflowModule {
		return NewGenericTestWorkflowModule()
	})
	if testing.Short() {
		os.Exit(0)
	}
	os.Exit(m.Run())
}
