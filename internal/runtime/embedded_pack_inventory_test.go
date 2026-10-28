package runtime_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func embeddedTriggerCatalog(t testing.TB) *providertriggers.CatalogSnapshot {
	t.Helper()
	return packfixture.TriggerCatalog(t)
}
