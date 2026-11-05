package bootverify

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/templatefanin"
)

func TestRunAcceptsCanonicalStreamAccumulatorProducerPath(t *testing.T) {
	bundle := templatefanin.LoadBundle(t)
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	for _, finding := range report.HardInvalidities() {
		if finding.CheckID == checkIDAccumulatorInputProducer || strings.Contains(finding.Message, "no accepted producer/source path") {
			t.Fatalf("canonical stream producer path was rejected: %#v", report.HardInvalidities())
		}
	}
}
