// Package deliveryfixture exposes selected-store delivery mechanics only to
// tests that need a transaction-backed lifecycle owner. Production code
// consumes the typed deliverylifecycle.Store surface instead.
package deliveryfixture

import deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"

type Adapter struct {
	*deliveryadapter.Adapter
}

type Dialect = deliveryadapter.Dialect

const (
	DialectPostgres = deliveryadapter.DialectPostgres
	DialectSQLite   = deliveryadapter.DialectSQLite
)

func NewAdapter(dialect Dialect) (*Adapter, error) {
	owner, err := deliveryadapter.NewAdapter(dialect)
	if err != nil {
		return nil, err
	}
	return &Adapter{Adapter: owner}, nil
}
