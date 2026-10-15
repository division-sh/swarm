package cliapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

func TestRuntimeOptionsDoNotSelectPlatformSpec(t *testing.T) {
	for _, options := range []any{
		CLISourcePathOptions{}, CLISourcePaths{}, ServeOptions{},
		runCommandOptions{}, verifyCommandOptions{}, describeCommandOptions{}, describeRoutesCommandOptions{},
		packCommandOptions{}, TestSessionRequest{},
		apiv1.Options{}, runtime.BundleContext{}, runforkexecution.SourceArtifactSelectedContractSourceLoader{},
	} {
		typ := reflect.TypeOf(options)
		t.Run(typ.Name(), func(t *testing.T) {
			for _, field := range []string{"PlatformSpecPath", "platformSpecPath"} {
				if _, exists := typ.FieldByName(field); exists {
					t.Fatalf("%s.%s still selects the platform spec; runtime commands must use embedded bytes", typ.Name(), field)
				}
			}
		})
	}
}
