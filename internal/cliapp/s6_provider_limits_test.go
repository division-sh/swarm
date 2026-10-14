package cliapp

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestS6UnifiedProviderLimitsRejectRetiredFields(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	for _, key := range []string{"rate_limit", "rate_limit_max_wait", "max_concurrency_max_wait", "models"} {
		for _, value := range []string{"null", "''", "{}", "[]", "0", "false"} {
			for _, merged := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/merged_%t", key, value, merged), func(t *testing.T) {
					policy := fmt.Sprintf("{%s: %s, max_concurrency: 2}", key, value)
					if merged {
						policy = fmt.Sprintf("{<<: {%s: %s}, max_concurrency: 2}", key, value)
					}
					path := filepath.Join(t.TempDir(), "swarm.yaml")
					writeRuntimeConfigText(t, path, "llm: {provider_limits: {anthropic: "+policy+"}}\n")
					_, err := loadUnifiedConfigForTest(t, unifiedConfigLoadOptions{ExplicitPath: path})
					if err == nil || !strings.Contains(err.Error(), key) {
						t.Fatalf("retired field %s admitted: %v", key, err)
					}
				})
			}
		}
	}
	for _, key := range []string{"rate_limit", "rate_limit_max_wait", "max_concurrency_max_wait", "models", "models.regular.max_concurrency"} {
		if _, admitted := unifiedConfigProviderLimitRule(strings.Split("llm.provider_limits.anthropic."+key, ".")); admitted {
			t.Fatalf("CLI vocabulary advertises retired policy %s", key)
		}
	}
}
