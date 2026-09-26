package pipeline

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestDeploymentFeedEmitIntentPreservesRootIdentityAndNumber(t *testing.T) {
	const digest = "0000000000000000000000000000000000000000000000000000000000000000"
	declaration := durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + digest)
	request := fanoutobligation.IntentRequest{
		Key:         fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()},
		Deployment:  &fanoutobligation.DeploymentOrigin{BundleHash: "bundle-v2:sha256:" + digest, Declaration: declaration, VersionID: version, SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + digest)},
		Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: declaration, VersionID: version},
		Cardinality: 2,
	}
	now := time.Now().UTC()
	intent := fanoutobligation.Intent{Request: request, Source: request.Source, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	item := map[string]any{"amount": json.Number("1.25")}
	first, err := deploymentFeedEmitIntent(intent, item, 0, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := deploymentFeedEmitIntent(intent, item, 0, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deploymentFeedEmitIntent(intent, item, 1, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	if first.Event.ID() != replayed.Event.ID() || first.Event.ID() == second.Event.ID() {
		t.Fatal("deployment ordinal identity was not stable and distinct")
	}
	if first.Event.RunID() != request.Key.RunID || first.Event.ParentEventID() != "" ||
		!bytes.Contains(first.Event.Payload(), []byte(`1.25`)) {
		t.Fatal("deployment root event lost run, causal absence or exact double")
	}
}
