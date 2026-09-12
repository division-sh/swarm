package runtimepersistence

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
)

func TestSelectedFanOutFutureSuccessArtifactPreserved(t *testing.T) {
	raw, err := os.ReadFile("testdata/future_capabilities/fork_selected_fan_out_success_test.go.txt")
	if err != nil {
		t.Fatal(err)
	}
	const want = "f64bbfa086c788802ab328623150377eb52e1b73e991b2db90c401eb81efe490"
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != want {
		t.Fatalf("future selected fan-out success oracle changed: got %s want %s", got, want)
	}
}
