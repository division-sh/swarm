package cliapp

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestScaffoldAdmittedArchetypesAndTeachNextCommands(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("internal/cliapp/archetypes/webhook-responder"),
		canonicalrouting.ArtifactID("internal/cliapp/archetypes/zero-agent-automation"),
	)
	for _, archetype := range []string{"zero-agent-automation", "webhook-responder"} {
		t.Run(archetype, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), archetype)
			var out bytes.Buffer
			if err := scaffoldArchetype(&out, archetype, destination); err != nil {
				t.Fatal(err)
			}
			for _, required := range []string{"package.yaml", "swarm.yaml", ".swarm/swarm.yaml", "tests/smoke.yaml"} {
				if _, err := os.Stat(filepath.Join(destination, required)); err != nil {
					t.Fatalf("missing %s: %v", required, err)
				}
			}
			for _, command := range []string{"swarm verify", "swarm serve", "swarm test"} {
				if !strings.Contains(out.String(), command) {
					t.Fatalf("output %q does not teach %s", out.String(), command)
				}
			}
			if err := scaffoldArchetype(&out, archetype, destination); err == nil || !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("existing destination error = %v", err)
			}
		})
	}
}

func TestScaffoldRejectsUnadmittedArchetype(t *testing.T) {
	if err := scaffoldArchetype(&bytes.Buffer{}, "approval-gate", filepath.Join(t.TempDir(), "approval")); err == nil || !strings.Contains(err.Error(), "admitted archetypes") {
		t.Fatalf("unadmitted archetype error = %v", err)
	}
}
