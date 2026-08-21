package runtime_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func embeddedTriggerCatalog(t testing.TB) *providertriggers.CatalogSnapshot {
	t.Helper()
	return packfixture.TriggerCatalog(t)
}

func embeddedChannelPacks(t testing.TB) []packs.LoadedChannelPack {
	t.Helper()
	return packfixture.ChannelPacks(t)
}

func embeddedConnectorRegistry(t testing.TB) *providerconnectors.PackRegistry {
	t.Helper()
	return packfixture.ConnectorRegistry(t)
}

func embeddedConnectorTool(t testing.TB, provider, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	t.Helper()
	installed := packfixture.ConnectorTool(t, provider, toolID)
	return installed.Tool, true
}
