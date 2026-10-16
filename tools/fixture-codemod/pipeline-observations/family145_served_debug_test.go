package main

import (
	_ "embed"
	"encoding/json"
)

//go:embed family145-served-debug-preimages.json
var family145ServedDebugPreimages []byte

type servedDebugPreimage struct {
	File, Function, SHA256, Before, After string
	DebugCalls, HelperCalls               int
}

var family145ServedDebugRows = func() []servedDebugPreimage {
	var rows []servedDebugPreimage
	if err := json.Unmarshal(family145ServedDebugPreimages, &rows); err != nil {
		panic(err)
	}
	return rows
}()

// Only the exact reviewed current body composes back to the signed predecessor.
// Mutated arguments, assertions, captures or control flow never enter this path.
func family145ServedDebugCallerInverse(function, after string) string {
	after = family145RunPhysicalCallerInverse(function, after)
	after = family145RequestKeyCallerInverse(function, after)
	after = family145ChannelIntentCallerInverse(function, after)
	after = family145ForkNoticeCallerInverse(function, after)
	after = family145ForkCompanionCallerInverse(function, after)
	after = family145PublicJoinCallerInverse(function, after)
	after = family145ReceiverReadbackCallerInverse(function, after)
	after = family145PublicA2EndpointCallerInverse(function, after)
	for _, row := range family145ServedDebugRows {
		if row.Function == function && row.After != "" && after == row.After {
			return row.Before
		}
	}
	return after
}
