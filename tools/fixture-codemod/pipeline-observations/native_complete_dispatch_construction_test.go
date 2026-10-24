package main

import (
	"strings"
)

func nativeCompleteDispatchConstruction(source string) string {
	source = strings.Replace(source, "\tvar db *sql.DB\n", "", 1)
	source = strings.Replace(source, "\t\tsqlite := storetest.StartSQLiteRuntimeStore(t)\n\t\tselected, db = sqlite, storetest.DatabaseForTest(sqlite)", "\t\tselected = storetest.StartSQLiteRuntimeStore(t)", 1)
	source = strings.Replace(source, "\t\t_, postgresDB, cleanup := testutil.StartPostgres(t)\n\t\tt.Cleanup(cleanup)\n\t\tpostgres := storetest.AdmitPostgresRuntimeStore(t, postgresDB)\n\t\tselected, db = postgres, postgresDB", "\t\tselected = storetest.StartPostgresRuntimeStore(t)", 1)
	return strings.Replace(source, "store: selected, db: db, dialect: backend,", "store: selected,", 1)
}
