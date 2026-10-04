package bootverify

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRunReplyBoundaryRejectsMalformedCorrelation(t *testing.T) {
	for _, rootRequester := range []bool{true, false} {
		for _, renamed := range []bool{false, true} {
			for _, mutation := range []canonicalrouting.RootReplyNegativeMutation{
				canonicalrouting.RootReplyCorrelationAbsent,
				canonicalrouting.RootReplyCorrelationOptional,
				canonicalrouting.RootReplyCorrelationList,
				canonicalrouting.RootReplyCorrelationIncompatible,
			} {
				t.Run(fmt.Sprintf("requester_%t/renamed_%t/mutation_%d", rootRequester, renamed, mutation), func(t *testing.T) {
					root := canonicalrouting.CopyRootReplyBoundary(t, rootRequester, true)
					if renamed {
						canonicalrouting.RenameRootReplyEndpoints(t, root, rootRequester)
					}
					if report := runReplyBoundaryVerification(t, root); report.HasErrors() {
						t.Fatalf("valid base rejected: %+v", report.Errors())
					}
					canonicalrouting.ApplyRootReplyBoundaryNegativeMutation(t, root, rootRequester, mutation)
					if report := runReplyBoundaryVerification(t, root); !reportContains(report.Errors(), "composition_connect_validation", "correlation_key") {
						t.Fatalf("missing canonical correlation rejection: %+v", report.Errors())
					}
				})
			}
		}
	}
}

func TestRunReplyBoundaryRejectsSchemaOnlyLocalConsumer(t *testing.T) {
	for _, rootRequester := range []bool{true, false} {
		for _, reply := range []bool{false, true} {
			t.Run(fmt.Sprintf("requester_%t/reply_%t", rootRequester, reply), func(t *testing.T) {
				root, owner := canonicalrouting.CopyRootReplyConsumerBoundary(t, rootRequester, reply, false, false)
				if report := runReplyBoundaryVerification(t, root); report.HasErrors() {
					t.Fatalf("valid base rejected: %+v", report.Errors())
				}
				restore := canonicalrouting.RemoveRootReplyConsumer(t, owner, rootRequester, false)
				if report := runReplyBoundaryVerification(t, root); !reportContains(report.Errors(), "composition_connect_validation", "requires a genuine consumer") {
					t.Fatalf("schema-only receiver accepted: %+v", report.Errors())
				}
				restore()
				if report := runReplyBoundaryVerification(t, root); report.HasErrors() {
					t.Fatalf("restored consumer rejected: %+v", report.Errors())
				}
			})
		}
	}
}

func runReplyBoundaryVerification(t testing.TB, root string) Report {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return Run(context.Background(), semanticview.Wrap(bundle), Options{})
}
