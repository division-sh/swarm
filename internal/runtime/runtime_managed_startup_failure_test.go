package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestFatalDeliveryContinuationRevokesRuntimeAdmissionAndServingGeneration(t *testing.T) {
	servingCtx, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{cancelStart: cancel, startCtx: servingCtx}

	runtime.failDeliveryContinuation(errors.New("fatal selected-store continuation failure"))

	if !runtime.shutdownAdmissionClosed() {
		t.Fatal("fatal delivery continuation left runtime admission open")
	}
	select {
	case <-servingCtx.Done():
	default:
		t.Fatal("fatal delivery continuation left serving generation active")
	}
}
