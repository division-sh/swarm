package bus_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestRootPinCannotSatisfyUnconnectedPrivateSourceInput(t *testing.T) {
	for _, handler := range []string{"thing.created", "thing.*"} {
		for _, rootPin := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/root_pin=%t", handler, rootPin), func(t *testing.T) {
				root := t.TempDir()
				rootSchema := "stages: []\n"
				if rootPin {
					rootSchema += "pins:\n  inputs:\n    - thing.created\n"
				}
				for path, body := range map[string]string{
					"manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
					"child/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
					"schema.yaml":         rootSchema,
					"events.yaml":         "thing.created:\n",
					"child/schema.yaml":   "pins:\n  inputs:\n    - thing.created\n",
					"child/nodes.yaml":    fmt.Sprintf("observer:\n  %s:\n    guard: true\n", handler),
				} {
					file := filepath.Join(root, path)
					if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(file, []byte(body), 0600); err != nil {
						t.Fatal(err)
					}
				}
				repo := filepath.Clean("../../..")
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
				if bundle != nil || !errors.Is(err, runtimecontracts.ErrInvalidField) ||
					!strings.Contains(err.Error(), fmt.Sprintf("handler %q", handler)) || !strings.Contains(err.Error(), "undeclared event") {
					t.Fatalf("unconnected private handler admission = %v, want receiver-local declaration refusal", err)
				}
			})
		}
	}
}
