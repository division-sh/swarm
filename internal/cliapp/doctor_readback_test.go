package cliapp

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
)

func TestDoctorInstalledReadbackScopesAdmissionBlockers(t *testing.T) {
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("preexisting-blocker", func(t *testing.T) {
		report := LocalPreflightReport{}
		report.add(localPreflightEnvPrerequisite, "unknown_stale", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, "stale environment source", "unset it")
		before := append([]localPreflightFinding(nil), report.Findings...)
		if !appendDoctorInstalledReadback(&report, base, true) {
			t.Fatal("admitted installed readback treated a preexisting blocker as its own failure")
		}
		if !report.HasBlockers() || !reflect.DeepEqual(report.Findings[:len(before)], before) || report.PackInventory == nil || report.SchemaInventory == nil || len(report.CapabilitySubjects) == 0 {
			t.Fatalf("readback lost the blocker or installed inventory: %#v", report)
		}
	})
	t.Run("new-capability-blocker", func(t *testing.T) {
		report := LocalPreflightReport{CapabilitySubjects: []packs.Subject{{}}}
		if appendDoctorInstalledReadback(&report, base, false) || !localPreflightReportHasCode(report, "provider_capability_model_invalid") {
			t.Fatalf("new capability-admission failure did not refuse readback: %#v", report)
		}
	})
	for _, schemaInventory := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalid-admission", true: "invalid-admission-with-schema-inventory"}[schemaInventory], func(t *testing.T) {
			report := LocalPreflightReport{}
			if appendDoctorInstalledReadback(&report, &packartifact.PlatformPackInventory{}, schemaInventory) || !report.HasBlockers() {
				t.Fatalf("invalid installed readback did not fail closed: %#v", report)
			}
			if len(report.Findings) != 1 || report.Findings[0].Category != localPreflightProviderPackPrerequisite ||
				report.Findings[0].Code != "platform_pack_load_failed" || report.Findings[0].Status != LocalPreflightStatusFailed ||
				report.Findings[0].Severity != LocalPreflightSeverityBlocker || report.Findings[0].Message != "selected platform pack base inventory is required" {
				t.Fatalf("invalid inventory lost its provider-pack blocker: %#v", report)
			}
			if (report.SchemaInventory != nil) != schemaInventory || report.PackInventory != nil {
				t.Fatalf("invalid pack admission changed schema readback or published a pack inventory: %#v", report)
			}
		})
	}
}
