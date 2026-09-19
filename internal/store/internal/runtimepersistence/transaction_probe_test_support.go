package runtimepersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// InstallTransactionProbeForTest exposes closed measurement through storetest,
// not a production selected-store interface or a raw transaction callback.
func InstallTransactionProbeForTest(selected any, options transactiontest.Options) (*transactiontest.Collector, func(), error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil {
			return store.backend.InstallTransactionProbeForTest(options)
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil {
			return store.backend.InstallTransactionProbeForTest(options)
		}
	}
	return nil, nil, fmt.Errorf("transaction probe requires a selected store, got %T", selected)
}
