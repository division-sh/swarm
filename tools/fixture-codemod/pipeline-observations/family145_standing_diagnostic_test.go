package main

import (
	_ "embed"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/standingdiagnostic"
)

//go:embed family145-standing-diagnostic-preimages.json
var standingDiagnosticBytes []byte
var standingDiagnosticManifest = func() standingdiagnostic.Manifest {
	manifest, err := standingdiagnostic.Decode(standingDiagnosticBytes)
	if err != nil {
		panic(err)
	}
	return manifest
}()

func TestFamily145StandingDiagnosticOwnersKeepThreeCutsAndDisplayFailurePolicies(t *testing.T) {
	for _, endpoint := range standingDiagnosticManifest.Endpoints {
		current, err := os.ReadFile(filepath.Join("../../..", endpoint.File))
		if err != nil || requestKeyDeclaration(t, current, endpoint.Function) != endpoint.After {
			t.Fatal("diagnostic owner, exact query, cuts or display policy changed:", endpoint.Function, err)
		}
		for _, cut := range []string{"OpenReleaseProcessReadOnlyInspection", "defer owner.Close()", "InspectSnapshot", "ORDER BY event_name", "ORDER BY created_at", "payload::text", "COALESCE(status, '')", "rows.Err()", "rows.Close()", "rows.Scan", "!postgres", "return nil,", "err == nil", "strings.Join(parts, \",\")"} {
			if !strings.Contains(endpoint.After, cut) {
				continue
			}
			mutant := strings.Replace(endpoint.After, cut, "unreviewedDiagnosticCut", 1)
			if diagnosticEndpointMatches(endpoint.File, endpoint.Function, mutant) {
				t.Fatal("changed owner/scope/order/scan/query policy accepted:", endpoint.Function, cut)
			}
		}
	}
}

func diagnosticEndpointMatches(file, function, current string) bool {
	for _, endpoint := range standingDiagnosticManifest.Endpoints {
		if endpoint.File == file && endpoint.Function == function {
			return current == endpoint.After
		}
	}
	return false
}
