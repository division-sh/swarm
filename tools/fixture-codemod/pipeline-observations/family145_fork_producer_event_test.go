package main

import (
	"fmt"
	"strings"
)

var family145ForkProducerCallers = map[string]int{
	"runForkReceiverOwnershipJourney":                                                      3,
	"TestSelectedForkIndependentReceiverBusinessMutationBothStores":                        2,
	"TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores": 2,
	"requireLifecycleRootLoopForkRevision":                                                 1,
}

func family145ForkProducerCallerInverse(function, after string) (string, error) {
	const native = "readForkReceiverProducerEvidence(t, rt.Runtime.Bus.Store(), "
	want := family145ForkProducerCallers[function]
	if want == 0 || strings.Count(after, native) != want || strings.Contains(after, "readForkReceiverProducerEvidence(t, rt, ") {
		return "", fmt.Errorf("producer evidence lost the original selected bus owner/call count: %s", function)
	}
	return strings.ReplaceAll(after, native, "readForkReceiverProducerEvidence(t, rt, "), nil
}
