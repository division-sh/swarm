package effectpersistence

type recoveryReadMode struct {
	inspection bool
	available  bool
}

var recoveryInspection = recoveryReadMode{inspection: true, available: true}

func recoveryLocks(mode []recoveryReadMode) bool {
	return len(mode) == 0 || !mode[0].inspection
}

func recoveryAvailable(mode []recoveryReadMode) bool {
	return len(mode) == 0 || mode[0].available
}
