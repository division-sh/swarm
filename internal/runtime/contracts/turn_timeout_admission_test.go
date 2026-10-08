package contracts

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestIssue2269TurnTimeoutDeclarationAdmission(t *testing.T) {
	for _, row := range []struct {
		name, value string
		accepted    bool
	}{
		{"bounded", "{after: 10m, emit: investigation.aborted}", true},
		{"precise", "{after: 1250ms, emit: investigation.aborted}", true},
		{"bare", "", false},
		{"null", "null", false},
		{"empty", "{}", false},
		{"scalar", "10m", false},
		{"missing_after", "{emit: investigation.aborted}", false},
		{"missing_emit", "{after: 10m}", false},
		{"zero", "{after: 0s, emit: investigation.aborted}", false},
		{"negative", "{after: -1m, emit: investigation.aborted}", false},
		{"numeric", "{after: 100, emit: investigation.aborted}", false},
		{"unknown", "{after: 10m, emit: investigation.aborted, reset: true}", false},
		{"blank_emit", "{after: 10m, emit: ''}", false},
		{"padded_emit", "{after: 10m, emit: ' investigation.aborted '}", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			body := "worker:\n  intent: {inline: investigate}\n  turn_timeout: " + row.value + "\n"
			path := filepath.Join(t.TempDir(), "agents.yaml")
			writeFixtureFile(t, path, body)
			disk, diskErr := loadOptionalAgentDeclarations(path)
			reconstructed, sourceErr := admitW5Agents(t, body)
			if (diskErr == nil) != row.accepted || (sourceErr == nil) != row.accepted {
				t.Fatalf("accepted=%t disk=%v source=%v", row.accepted, diskErr, sourceErr)
			}
			if !row.accepted {
				if !strings.Contains(diskErr.Error(), "turn_timeout") || !strings.Contains(sourceErr.Error(), "turn_timeout") {
					t.Fatalf("refusal lost declaration coordinate: disk=%v source=%v", diskErr, sourceErr)
				}
				return
			}
			diskEntry, sourceEntry := disk["worker"], reconstructed["worker"]
			if diskEntry.TurnTimeout == nil || diskEntry.TurnTimeout.Validate() != nil || diskEntry.TurnTimeout.Emit != "investigation.aborted" {
				t.Fatalf("lost typed timeout: %+v", diskEntry)
			}
			if row.name == "bounded" && diskEntry.TurnTimeout.After != 10*time.Minute || row.name == "precise" && diskEntry.TurnTimeout.After != 1250*time.Millisecond {
				t.Fatalf("duration changed: %+v", diskEntry.TurnTimeout)
			}
			effective := EffectiveAgentRegistryEntry("worker", diskEntry)
			if effective.TurnTimeout == diskEntry.TurnTimeout || effective.EffectiveFieldSources["turn_timeout"] != AgentFieldSourceAuthored {
				t.Fatal("effective declaration lost timeout ownership or aliases its source")
			}
			diskEntry.admissionProvenance, sourceEntry.admissionProvenance = nil, nil
			if !reflect.DeepEqual(diskEntry, sourceEntry) {
				t.Fatalf("loaders disagree: disk=%+v source=%+v", diskEntry, sourceEntry)
			}
		})
	}
}

func TestIssue2269TurnTimeoutEventBindingDiskAndRetained(t *testing.T) {
	repo := repoRootForContractsTest(t)
	for _, row := range []struct {
		name, events string
		accepted     bool
	}{
		{"bare", "investigation.aborted:\n", true},
		{"optional", "investigation.aborted:\n  detail: text?\n", true},
		{"unknown", "other.event:\n", false},
		{"requires_payload", "investigation.aborted:\n  detail: text\n", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: timeout-bound\nstages:\n  waiting: {}\n  done: {final: true}\n")
			writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "worker:\n  intent: {inline: investigate}\n  turn_timeout: {after: 10m, emit: investigation.aborted}\n")
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), row.events)
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			retained, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			disk, diskErr := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			loaded, loadedErr := LoadWorkflowContractBundleFromArtifact(repo, retained, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
			if (diskErr == nil) != row.accepted || (loadedErr == nil) != row.accepted {
				t.Fatalf("accepted=%t disk=%v retained=%v", row.accepted, diskErr, loadedErr)
			}
			if !row.accepted {
				if !strings.Contains(diskErr.Error(), "turn_timeout.emit") || !strings.Contains(loadedErr.Error(), "turn_timeout.emit") {
					t.Fatalf("refusal did not identify binding: %v / %v", diskErr, loadedErr)
				}
				return
			}
			left, right := disk.AgentDeclarationRecords(), loaded.AgentDeclarationRecords()
			if !reflect.DeepEqual(left, right) || len(left) != 1 || len(left[0].Entry.EmitEvents) != 0 || !reflect.DeepEqual(left[0].Entry.ProducedEvents(), []string{"investigation.aborted"}) {
				t.Fatalf("timeout changed source or granted an emit tool: %+v / %+v", left, right)
			}
		})
	}
}
