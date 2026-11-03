package effectpersistence

type recoveryReadMode bool

const recoveryInspection recoveryReadMode = true

func recoveryLocks(mode []recoveryReadMode) bool {
	return len(mode) == 0 || !bool(mode[0])
}
