package fanoutobligation

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

const sixtyFourZeroes = "0000000000000000000000000000000000000000000000000000000000000000"

func deploymentRequest() IntentRequest {
	declaration := durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + sixtyFourZeroes)
	return IntentRequest{
		Key: IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()},
		Deployment: &DeploymentOrigin{
			BundleHash:   "bundle-v2:sha256:" + sixtyFourZeroes,
			Declaration:  declaration,
			VersionID:    version,
			SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + sixtyFourZeroes),
		},
		Source:      SourceRef{Kind: SourceResourceVersion, Declaration: declaration, VersionID: version},
		Cardinality: 2,
	}
}

func TestDeploymentOriginClosedUnionAndZeroRow(t *testing.T) {
	request := deploymentRequest()
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	request.Cardinality = 0
	if err := request.Validate(); err != nil {
		t.Fatalf("zero-row feed rejected: %v", err)
	}
	now := time.Now().UTC()
	intent := Intent{Request: request, Source: request.Source, Status: StatusClosed, NextChunkSize: InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	if err := intent.Validate(); err != nil {
		t.Fatalf("zero-row terminal fact rejected: %v", err)
	}
	if intent.ChunkEndOrdinal() != 0 {
		t.Fatal("zero-row feed owes an ordinal")
	}
	request.Cardinality = 2
	intent.Request = request
	intent.Status = StatusOpen
	if err := intent.Validate(); err != nil {
		t.Fatalf("nonempty deployment feed rejected: %v", err)
	}
}

func TestDeploymentOriginRejectsMixedOrIncompleteIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*IntentRequest)
	}{
		{"delivery", func(r *IntentRequest) { r.Key.TriggeringDeliveryID = uuid.NewString() }},
		{"authored element", func(r *IntentRequest) { r.Key.ElementRef = validIntentRequest(t).Key.ElementRef }},
		{"handler plan", func(r *IntentRequest) { r.PlanRef = validIntentRequest(t).PlanRef }},
		{"handler capsule", func(r *IntentRequest) { r.Capsule = validIntentRequest(t).Capsule }},
		{"missing feed", func(r *IntentRequest) { r.Key.DeploymentFeedID = "" }},
		{"noncanonical feed", func(r *IntentRequest) { r.Key.DeploymentFeedID = "not-a-uuid" }},
		{"noncanonical run", func(r *IntentRequest) { r.Key.RunID = " " + r.Key.RunID }},
		{"missing version", func(r *IntentRequest) { r.Deployment.VersionID = "" }},
		{"padded version", func(r *IntentRequest) {
			r.Deployment.VersionID = durabledata.VersionID(" " + string(r.Deployment.VersionID))
			r.Source.VersionID = r.Deployment.VersionID
		}},
		{"different source version", func(r *IntentRequest) {
			r.Source.VersionID = durabledata.VersionID("resource-version-v1:sha256:" + "1111111111111111111111111111111111111111111111111111111111111111")
		}},
		{"wrong source kind", func(r *IntentRequest) { r.Source = validIntentRequest(t).Source }},
		{"missing schema", func(r *IntentRequest) { r.Deployment.SchemaDigest = "" }},
		{"missing bundle", func(r *IntentRequest) { r.Deployment.BundleHash = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := deploymentRequest()
			tc.edit(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("contradictory deployment origin admitted")
			}
		})
	}
	handler := validIntentRequest(t)
	handler.Key.DeploymentFeedID = uuid.NewString()
	if err := handler.Validate(); err == nil {
		t.Fatal("handler borrowed deployment feed identity")
	}
}

func TestHandlerOriginCannotBorrowDeploymentResourceSource(t *testing.T) {
	request := validIntentRequest(t)
	resource := deploymentRequest()
	request.Source = resource.Source
	if err := request.Validate(); err == nil {
		t.Fatal("handler-origin resource source was accepted without deployment feed identity")
	}
}
