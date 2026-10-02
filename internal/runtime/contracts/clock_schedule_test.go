package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClockScheduleSchemaAdmission(t *testing.T) {
	source := "schedules:\n  morning_report: {cron: \"0 9 * * *\", emit: report.requested}\n  poll: {every: 5m, emit: poll.tick}\n"
	schema, err := admitSchemaFragment(source)
	if err != nil || len(schema.Schedules) != 2 || schema.Schedules["poll"].Every != "5m" {
		t.Fatalf("minimal declaration = %#v, %v", schema.Schedules, err)
	}
	if got := schema.admissionProvenance[`schedules.poll.every`]; got.Origin != EffectiveValueOriginAuthored {
		t.Fatalf("cadence provenance lost: %#v", schema.admissionProvenance)
	}
	var direct FlowSchemaDocument
	if err := decodeNodeTestYAML([]byte(source), &direct); err != nil || direct.Schedules["morning_report"].Cron != "0 9 * * *" {
		t.Fatalf("direct typed admission = %#v, %v", direct.Schedules, err)
	}
}

func TestClockScheduleStrictPresenceAndExpansion(t *testing.T) {
	for _, entry := range []string{
		"null", "''", "false", "[]", "{}", "5m",
		"{emit: poll.tick}", "{every: 5m}", "{cron: '0 9 * * *', every: 5m, emit: poll.tick}",
		"{every: null, emit: poll.tick}", "{every: '', emit: poll.tick}", "{every: false, emit: poll.tick}",
		"{every: 5m, emit: poll.tick, payload: {}}", "{every: 5m, emit: poll.tick, timezone: UTC}",
		"{every: 5m, emit: ' poll.tick'}", "{every: 5m, emit: /poll.tick}", "{every: 5m, emit: poll.tick/}",
		"{every: 0s, emit: poll.tick}", "{every: -1s, emit: poll.tick}", "{every: 1ns, emit: poll.tick}",
		"{every: 1500ns, emit: poll.tick}", "{every: 999999999999999999999h, emit: poll.tick}",
		"{cron: '@daily', emit: poll.tick}", "{cron: '@every 5m', emit: poll.tick}",
		"{cron: 'CRON_TZ=UTC 0 9 * * *', emit: poll.tick}", "{cron: '0 0 31 2 *', emit: poll.tick}",
		"{every: 5m, every: 1m, emit: poll.tick}",
	} {
		t.Run(entry, func(t *testing.T) {
			_, err := admitSchemaFragment("schedules:\n  poll: " + entry + "\n")
			if err == nil || !strings.Contains(err.Error(), "schema.yaml:") {
				t.Fatalf("invalid declaration admitted or source location lost: %v", err)
			}
		})
	}
	for _, source := range []string{
		"schedules: null\n", "schedules: {}\n", "schedules: []\n",
		"schedules:\n  poll: {every: 5m, emit: poll.tick}\n  poll: {every: 1m, emit: poll.tick}\n",
		"schedules: &clocks\n  poll: *clocks\n",
	} {
		if _, err := admitSchemaFragment(source); err == nil {
			t.Fatalf("invalid root shape admitted: %s", source)
		}
	}
	_, err := admitSchemaFragment("schedules:\n  poll: &clock {every: 5m, emit: poll.tick}\n  other: {<<: *clock, payload: {}}\n")
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("expanded unknown field bypassed admission: %v", err)
	}
	if _, err := admitSchemaFragment("schedules:\n  poll: &clock {every: 5m, emit: poll.tick}\n  other: *clock\n"); err != nil {
		t.Fatalf("lawful expanded declaration refused: %v", err)
	}
}

func TestClockScheduleRequiresKeylessAncestry(t *testing.T) {
	const clock = "schedules:\n  poll: {every: 5m, emit: poll.tick}\n"
	if _, err := admitSchemaFragment("instance: account_id\n" + clock); err == nil || !strings.Contains(err.Error(), "schedules fire per run") {
		t.Fatalf("keyed owner admitted: %v", err)
	}
	for _, keyed := range []string{"", ".", "account", "account/worker", "unrelated"} {
		t.Run(keyed, func(t *testing.T) {
			root := t.TempDir()
			for _, path := range []string{".", "account", "account/worker", "account/worker/poller", "unrelated"} {
				dir := filepath.Join(root, path)
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				schema := "stages: []\n"
				if path == keyed {
					schema += "instance: account_id\n"
					if err := os.WriteFile(filepath.Join(dir, "entities.yaml"), []byte("account:\n  account_id: text\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				if path == "account/worker/poller" {
					schema += clock
				}
				if err := os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte(schema), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := repoRootForContractsTest(t)
			_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			wantFailure := keyed != "" && keyed != "unrelated"
			if (err != nil) != wantFailure {
				t.Fatalf("keyed %q topology = %v", keyed, err)
			}
			if wantFailure && !strings.Contains(err.Error(), "schedules fire per run") {
				t.Fatalf("teaching lost: %v", err)
			}
			if _, err := LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(root, "account/worker/poller"), DefaultPlatformSpecFile(repo)); err != nil {
				t.Fatalf("same child selected standalone refused: %v", err)
			}
		})
	}
}
