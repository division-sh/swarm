package runtime

import "testing"

func skipEmpireCoupledPlatformizationBlocker(t *testing.T) {
	t.Helper()
	t.Skip("skipping Empire-coupled legacy test during MAS platformization; preserve for later product-owned relocation or rewrite")
}
