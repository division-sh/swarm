package store

import "database/sql"

func (s *PostgresStore) RuntimeSQLDB() *sql.DB {
	if s == nil {
		return nil
	}
	return s.DB
}

func (s *SQLiteRuntimeStore) RuntimeSQLDB() *sql.DB {
	if s == nil {
		return nil
	}
	return s.DB
}
