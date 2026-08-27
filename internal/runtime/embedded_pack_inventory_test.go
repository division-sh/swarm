package runtime_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func embeddedTriggerCatalog(t testing.TB) *providertriggers.CatalogSnapshot {
	t.Helper()
	return packfixture.TriggerCatalog(t)
}

func embeddedConnectorTool(t testing.TB, provider, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	t.Helper()
	installed := packfixture.ConnectorTool(t, provider, toolID)
	return installed.Tool, true
}
