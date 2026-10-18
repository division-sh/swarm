package storetest

import private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"

type OriginalProcessCapture = private.OriginalProcessCaptureForTest
type OriginalSessionInterruption = private.OriginalSessionInterruptionForTest

func CaptureOriginalProcess(selected any) (*OriginalProcessCapture, error) {
	return private.CaptureOriginalProcessForTest(selected)
}
