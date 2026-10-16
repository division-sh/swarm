package runtimepersistence

// A test-only scalar bridge to the existing exact-coordinator observer. No
// handle, transaction, callback or production fixture capability is exposed.
func RecoveryRunFaultQueuedWritersForTest(selected *SQLiteRuntimeStore) int {
	return selected.backend.QueuedWritersForTest()
}
