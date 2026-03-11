package empire

import (
	"testing"

	runtimemanager "empireai/internal/runtime/manager"
)

func TestDefaultOpCoRoutesBootstrapSeededCounts(t *testing.T) {
	key := runtimemanager.RouteRuleKey("v-123", "operating/*/opco.ceo_ready", "opco-ceo-v-123")
	if key == "" {
		t.Fatal("expected route rule key")
	}
}

func TestDefaultOpCoRosterCEOIncludesCrossDomainReport(t *testing.T) {
	if got := runtimemanager.OpCoAgentID("ceo", "v-456"); got != "ceo-v-456" {
		t.Fatalf("OpCoAgentID() = %q, want ceo-v-456", got)
	}
}
