package storetest

import private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"

type ChannelCallbackRollbackFault = private.ChannelCallbackRollbackFault

func OpenChannelCallbackRollbackFault(selected any, event string) (ChannelCallbackRollbackFault, error) {
	return private.OpenChannelCallbackRollbackFaultForTest(selected, event)
}
