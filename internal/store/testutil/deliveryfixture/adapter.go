// Package deliveryfixture exposes selected-store delivery mechanics only to
// tests that need a real transaction-backed lifecycle owner. Production code
// must consume the typed deliverylifecycle.Store surface instead.
package deliveryfixture

import deliveryadapter "github.com/division-sh/swarm/internal/store/internal/delivery"

type Adapter = deliveryadapter.Adapter
type Dialect = deliveryadapter.Dialect

const (
	DialectPostgres = deliveryadapter.DialectPostgres
	DialectSQLite   = deliveryadapter.DialectSQLite
)

func NewAdapter(dialect Dialect) (*Adapter, error) {
	return deliveryadapter.NewAdapter(dialect)
}
