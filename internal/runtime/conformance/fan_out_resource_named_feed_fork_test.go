package conformance

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestDeploymentSourceSelectedForkParksOtherFeedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/legacy=%t", backend, legacy), func(t *testing.T) {
				proveSelectedNamedFeedFork(t, backend, legacy)
			})
		}
	}
}

func proveSelectedNamedFeedFork(t *testing.T, backend string, legacy bool) {
	t.Helper()
	repo := conformanceRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyPublicCompletionFeedOnly(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
	f.runtime.fanOutServing.Close()
	names := []string{"scan.requested", "score.observed"}
	input := [][]byte{[]byte("{\"topic\":\"named\"}\n"), []byte("{\"label\":\"other\"}\n")}
	var sourceID, batchID string
	if legacy {
		var data durabledata.RunCreationDataEnvelope
		for i, name := range names {
			ref, err := durabledata.ParseDeclarationRef(".", name)
			if err != nil {
				t.Fatal(err)
			}
			data.Imports = append(data.Imports, durabledata.FusedImport{
				Declaration: ref, SourceInvocationID: uuid.NewString(),
				ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input[i],
			})
		}
		sourceID = createDeploymentResourceFixtureRun(t, f, data)
	} else {
		sourceID = uuid.NewString()
		prepareDeploymentRowsFixtureRun(t, f, sourceID, ".")
		server := f.operatorServer(t)
		args := []string{"run", "start", "--connect", server.URL, "--run-id", sourceID, "--idempotency-key", sourceID, "--no-follow"}
		dir := t.TempDir()
		for i, name := range names {
			path := filepath.Join(dir, name+".jsonl")
			if err := os.WriteFile(path, input[i], 0600); err != nil {
				t.Fatal(err)
			}
			args = append(args, "--data", name+"="+path)
		}
		var stdout, stderr bytes.Buffer
		if code := cliapp.Execute(f.ctx, args, &stdout, &stderr, nil, nil); code != 0 {
			t.Fatalf("commit source feed batch: exit=%d stderr=%s", code, stderr.String())
		}
		_, receipt, ok := strings.Cut(stdout.String(), "rows published: publication_id=")
		batchID, _, _ = strings.Cut(receipt, "\n")
		if _, err := uuid.Parse(batchID); !ok || err != nil {
			t.Fatalf("source batch receipt: %s err=%v", stdout.String(), err)
		}
	}
	planner := f.selected.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	before, err := planner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: sourceID, PublicationID: batchID})
	if err != nil || len(before.FanOutObligations) != 2 || before.EventCountAtFork != 0 {
		t.Fatalf("source cut must hold both unissued feeds: %+v err=%v", before, err)
	}
	var named, other runfork.RunForkFanOutObligation
	for _, o := range before.FanOutObligations {
		if o.Intent.Cursor != 0 || o.Intent.Request.Cardinality != 1 {
			t.Fatalf("source advanced before its fixed cut: %+v", o)
		}
		switch o.Intent.Source.Declaration.EventName {
		case names[0]:
			named = o
		case names[1]:
			other = o
		}
	}
	if named.Intent.Request.Key.DeploymentFeedID == "" || other.Intent.Request.Key.DeploymentFeedID == "" {
		t.Fatal("fixture lacks exact named and other source members")
	}
	pauseDeploymentFixtureRun(t, f, sourceID)
	owner, recovered := deploymentForkOwner(t, f)
	if len(recovered) != 0 {
		t.Fatalf("fresh selected owner found competing work: %+v", recovered)
	}
	server := deploymentForkServer(t, f, owner)
	params := map[string]any{
		"source_run_id": sourceID, "publication_id": named.Intent.Request.Key.DeploymentFeedID,
		"idempotency_key": uuid.NewString(),
	}
	result, refused := deploymentForkRPC(t, f.ctx, server, params)
	if len(refused) != 0 || result.ForkRunID == "" || result.ForkRevision != before.ForkPoint.Revision {
		t.Fatalf("named feed fork: %+v refusal=%s", result, refused)
	}
	availability := f.selected.(apiv1.RunForkAvailabilityStore)
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, err := availability.LoadRunBundleAvailability(f.ctx, result.ForkRunID)
		if err != nil {
			t.Fatal(err)
		}
		if state.Status == "completed" {
			break
		}
		if state.Status == "failed" || time.Now().After(deadline) {
			t.Fatalf("named feed did not settle: %+v", state)
		}
		time.Sleep(20 * time.Millisecond)
	}
	retry, refused := deploymentForkRPC(t, f.ctx, deploymentForkServer(t, f, owner), params)
	if len(refused) != 0 || !reflect.DeepEqual(retry, result) {
		t.Fatalf("reconstructed handler changed the acknowledged fork: %+v refusal=%s", retry, refused)
	}
	childBatchID := ""
	if batchID != "" {
		childBatchID = durabledata.ForkRowsPublicationID(result.ForkRunID, batchID)
	}
	childMemberID := (durabledata.DeploymentFeed{RunID: result.ForkRunID, Publication: childBatchID, Declaration: named.Intent.Source.Declaration}).PublicationID()
	var cursor int
	var selectedVersion durabledata.VersionID
	if err := f.db.QueryRowContext(f.ctx, `SELECT cursor,source_resource_version_id FROM fan_out_intents
		WHERE run_id=$1 AND deployment_feed_id=$2`, result.ForkRunID, childMemberID).Scan(&cursor, &selectedVersion); err != nil ||
		cursor != 1 || selectedVersion != named.Intent.Source.VersionID {
		t.Fatalf("exact child member %s: cursor=%d version=%s err=%v", childMemberID, cursor, selectedVersion, err)
	}
	var childFeeds, namedRows, otherEvents, sourceFeeds, childRuns int
	for _, check := range []struct {
		id, query string
		got       *int
	}{
		{result.ForkRunID, "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'", &childFeeds},
		{result.ForkRunID, "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_resource_event_name='scan.requested' AND cursor=1 AND status='closed'", &namedRows},
		{result.ForkRunID, "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='score.observed'", &otherEvents},
		{sourceID, "SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' AND cursor=0", &sourceFeeds},
		{sourceID, "SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1", &childRuns},
	} {
		if err := f.db.QueryRowContext(f.ctx, check.query, check.id).Scan(check.got); err != nil {
			t.Fatal(err)
		}
	}
	if childFeeds != 1 || namedRows != 1 || otherEvents != 0 || sourceFeeds != 2 || childRuns != 1 {
		t.Fatalf("named-only work or source custody changed: child feeds=%d named settled=%d other events=%d source feeds=%d children=%d", childFeeds, namedRows, otherEvents, sourceFeeds, childRuns)
	}
}
