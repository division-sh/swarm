package bus_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRootPinCannotSatisfyUnconnectedPrivateSourceInput(t *testing.T) {
	for _, mode := range []string{"static"} {
		for _, rootPin := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/root_pin=%t", mode, rootPin), func(t *testing.T) {
				root := t.TempDir()
				rootSchema := "name: root\n"
				if rootPin {
					rootSchema += "pins:\n  inputs:\n    - thing.created\n"
				}
				for path, body := range map[string]string{
					"schema.yaml":       rootSchema,
					"events.yaml":       "thing.created:\n",
					"child/schema.yaml": "name: child\npins:\n  inputs:\n    - thing.created\n",
					"child/nodes.yaml":  "observer:\n  execution_type: system_node\n  subscribes_to: [thing.created]\n  event_handlers:\n    thing.created:\n      guard: {id: admit, check: 'true'}\n",
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
				if err != nil {
					t.Fatal(err)
				}
				source := semanticview.Wrap(bundle)
				missing := false
				for _, finding := range bootverify.Run(context.Background(), source, bootverify.Options{}).HardInvalidities() {
					if finding.CheckID == "input_pin_wiring" && finding.Location == "child" {
						missing = true
					}
				}
				if !missing {
					t.Fatal("unconnected child passed input producer verification")
				}
				if _, err := runtimebus.DeriveRouteTable(source); err == nil || !strings.Contains(err.Error(), "does not resolve to receiver-local event") {
					t.Fatalf("unconnected child subscription admission = %v, want private receiver refusal", err)
				}
			})
		}
	}
}
