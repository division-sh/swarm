package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type prefilterContractVectors struct {
	Cases []prefilterContractCase `yaml:"cases"`
}

type prefilterContractCase struct {
	Name           string `yaml:"name"`
	Expected       string `yaml:"expected"`
	ExpectedReason string `yaml:"expected_reason"`
	Input          struct {
		SignalStrength      float64  `yaml:"signal_strength"`
		RedFlags            []string `yaml:"red_flags"`
		EvidenceURLs        int      `yaml:"evidence_urls"`
		RetentionPrimitives []string `yaml:"retention_primitives"`
	} `yaml:"input"`
}

func runPrefilterContractVectorChecks(t *testing.T, repoRoot string) {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "test-vectors", "prefilter.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read prefilter vectors: %v", err)
	}
	var vectors prefilterContractVectors
	if err := yaml.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse prefilter vectors: %v", err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatal("prefilter vectors empty")
	}

	for _, tc := range vectors.Cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			payload := buildPrefilterFixturePayload(tc)
			ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
			switch strings.ToLower(strings.TrimSpace(tc.Expected)) {
			case "pass":
				if !ok {
					t.Fatalf("expected pass, got reject reason=%q", reason)
				}
			case "reject":
				if ok {
					t.Fatalf("expected reject, got pass")
				}
				if want := strings.TrimSpace(tc.ExpectedReason); want != "" && reason != want {
					t.Fatalf("reject reason mismatch: got=%q want=%q", reason, want)
				}
			default:
				t.Fatalf("unsupported expected value %q", tc.Expected)
			}
		})
	}
}

func buildPrefilterFixturePayload(tc prefilterContractCase) map[string]any {
	urlCount := tc.Input.EvidenceURLs
	if urlCount <= 0 {
		urlCount = 1
	}
	urls := make([]string, 0, urlCount)
	for i := 0; i < urlCount; i++ {
		urls = append(urls, "https://example.com/vector/"+tc.Name+"/"+strings.TrimSpace(strings.ReplaceAll(uuid.NewString(), "-", "")))
	}
	urlAt := func(idx int) string {
		if len(urls) == 0 {
			return "https://example.com/vector/default"
		}
		return urls[idx%len(urls)]
	}

	redFlags := make([]any, 0, len(tc.Input.RedFlags))
	for _, flag := range tc.Input.RedFlags {
		flag = strings.TrimSpace(flag)
		if flag == "" {
			continue
		}
		redFlags = append(redFlags, map[string]any{"type": flag})
	}
	retention := make([]any, 0, len(tc.Input.RetentionPrimitives))
	for _, primitive := range tc.Input.RetentionPrimitives {
		primitive = strings.TrimSpace(primitive)
		if primitive == "" {
			continue
		}
		retention = append(retention, primitive)
	}
	return map[string]any{
		"signal_strength":        tc.Input.SignalStrength,
		"opportunity_name":       "Fixture Opportunity " + tc.Name,
		"preliminary_icp":        "Owner at salon schedule desk",
		"opportunity_hypothesis": "Simple booking helper",
		"retention_primitives":   retention,
		"opportunity_pattern":    "ai_wrapper",
		"build_sketch": map[string]any{
			"core_features":    []any{"Booking page"},
			"key_integrations": []any{},
			"red_flags":        redFlags,
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "Comp", "pricing": "$99", "source_url": urlAt(0)},
			},
			"pain_signals": []any{
				map[string]any{"signal": "Manual scheduling pain", "source_url": urlAt(1)},
			},
			"buyer_communities": []any{
				map[string]any{"name": "Salon owners", "source_url": urlAt(2)},
			},
			"regulatory": []any{
				map[string]any{"detail": "Local requirements", "source_url": urlAt(3)},
			},
		},
	}
}
