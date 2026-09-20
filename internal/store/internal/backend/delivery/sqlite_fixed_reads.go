package delivery

import sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"

// Slots belong to the delivery owner; successful DB-backed handles are closed
// by its backend. No observations, connections or transactions are retained.
type sqliteDeliveryReads struct {
	backend             *sqlitebackend.Backend
	singletonMembership sqlitebackend.FixedReadStatement
	singletonRecords    sqlitebackend.FixedReadStatement
}

func (a *Adapter) poolReads(q queryer) *sqliteDeliveryReads {
	if a.dialect == DialectSQLite && a.sqliteReads != nil {
		if b, ok := q.(*sqlitebackend.Backend); ok && b == a.sqliteReads.backend {
			return a.sqliteReads
		}
	}
	return nil
}
