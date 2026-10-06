package store_test

import (
	"testing"
)

func TestInboundSetupSeedDoesNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, finding := range findings {
		if unusedInboundSetupAuthority(finding) {
			t.Errorf("native inbound seed regained unused raw authority: %s", finding.registryLine())
		}
	}
}

func unusedInboundSetupAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	if finding.File == "internal/runtime/inbound_postgres_test.go" {
		return finding.Enclosing == "seedPostgresInboundGatewayRuntime"
	}
	if finding.File != "internal/runtime/inbound_raw_settlement_store_test.go" {
		return false
	}
	switch finding.Enclosing {
	case "seedProviderRawSettlementRuntime", "TestInboundGatewayProviderRawSettlementSQLitePostgres":
		return true
	case "openProviderRawSettlementStore":
		return finding.Kind == "raw-type" && finding.Member == "result:#2"
	}
	return false
}
