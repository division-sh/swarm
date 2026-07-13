package guardfixture

func hiddenSQLiteDefault(t testing.TB) string {
	return SQLitePath(t, SQLiteFreshFile())
}

func TestCallsHiddenSQLiteDefault(t *testing.T) {
	_ = hiddenSQLiteDefault(t)
}
