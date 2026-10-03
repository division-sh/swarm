package testplanning

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCITierEventDataAndConservativeDefault(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{"CI-Tier: core", ProfileCore}, {"heading\nCI-Tier: lifecycle\r\nLocal-Tier: full", ProfileLifecycle},
		{"CI-Tier: full", ProfileFull}, {"", ProfileFull}, {"CI-Tier: unknown", ProfileFull},
		{"CI-Tier:core", ProfileFull}, {" CI-Tier: core", ProfileFull},
		{"CI-Tier: core\nCI-Tier: lifecycle", ProfileFull}, {"CI-Tier: core\nCI-Tier: core", ProfileFull},
		{"CI-Tier: $(touch /tmp/unsafe)", ProfileFull}, {"CI-Tier: core\nCI-Tier: malformed", ProfileFull},
	} {
		t.Run(tc.body, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"pull_request": map[string]string{"body": tc.body}})
			body, err := PREventBody(raw)
			if err != nil {
				t.Fatal(err)
			}
			got, reason := CITier(body)
			if got != tc.want || reason == "" {
				t.Fatalf("tier=%s reason=%s", got, reason)
			}
		})
	}
	if _, err := PREventBody([]byte("{invalid")); err == nil {
		t.Fatal("malformed event admitted")
	}
}

func TestCITierCurrentBodyRejectsStaleThinnerGreen(t *testing.T) {
	for _, effective := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		for _, current := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			err := CheckCurrentCITier(effective, "CI-Tier: "+current)
			if (err == nil) != (TierRank(effective) >= TierRank(current)) {
				t.Fatalf("%s vs %s: %v", effective, current, err)
			}
		}
	}
	if CheckCurrentCITier(ProfileCore, "") == nil {
		t.Fatal("missing instruction admitted thin green")
	}
}

func TestFixedTierIsIndependentOfGitDelta(t *testing.T) {
	policy := testPolicy()
	for _, status := range []string{"A", "M", "D", "R100", "C100"} {
		for _, path := range []string{"README.md", "docs/guide.md", "platform-spec.yaml", "internal/runtime/a.go", ".github/audit-artifacts/a.md", "other/new.go"} {
			t.Run(status+"/"+path, func(t *testing.T) {
				got, _, err := policy.ResolveProfile("pull_request", "CI-Tier: core", "")
				if err != nil || got != ProfileCore {
					t.Fatalf("fixed tier=%s %v", got, err)
				}
			})
		}
	}
	if _, _, err := policy.ResolveProfile("pull_request", "CI-Tier: core", ProfileFull); err == nil || !strings.Contains(err.Error(), "workflow_dispatch") {
		t.Fatal("forced event bypass")
	}
}

func TestManualDispatchRequiresExhaustiveFull(t *testing.T) {
	policy := testPolicy()
	for _, forced := range []string{"", ProfileFull, ProfileCore, ProfileLifecycle, "unknown"} {
		got, _, err := policy.ResolveProfile("workflow_dispatch", "CI-Tier: core", forced)
		if forced == "" || forced == ProfileFull {
			if err != nil || got != ProfileFull {
				t.Fatalf("manual full %q: %s %v", forced, got, err)
			}
		} else if err == nil {
			t.Fatalf("thin/invalid manual dispatch admitted: %q -> %s", forced, got)
		}
	}
}
