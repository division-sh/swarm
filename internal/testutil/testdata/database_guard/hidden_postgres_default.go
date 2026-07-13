package guardfixture

func hiddenPostgresDefault(t testing.TB) {
	AcquirePostgres(t, PostgresRowState())
}

func TestCallsHiddenPostgresDefault(t *testing.T) {
	hiddenPostgresDefault(t)
}
