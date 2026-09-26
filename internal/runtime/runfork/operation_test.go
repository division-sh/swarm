package runfork

import (
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

func TestForkOperationCanonicalIdentityBindsSelectedMeaning(t *testing.T) {
	first := ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "same-key",
		TransportHash: "exact-wire-request", SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(),
		TargetBundleHash:  "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContractSelection: RunForkContractSelection{Mode: RunForkContractSelectionModeSelectedContracts},
		DataPinOverrides: []durabledata.ExplicitPin{
			{Declaration: durabledata.DeclarationRef{FlowPath: "z", EventName: "z/last"}, VersionID: durabledata.VersionID("resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
			{Declaration: durabledata.DeclarationRef{FlowPath: "a", EventName: "a/first"}, VersionID: durabledata.VersionID("resource-version-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")},
		},
	}
	canonical, hash, err := first.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if canonical.DataPinOverrides[0].Declaration.FlowPath != "a" {
		t.Fatalf("pin order not canonical: %+v", canonical.DataPinOverrides)
	}
	reordered := first
	reordered.OperationID = uuid.NewString()
	reordered.DataPinOverrides = []durabledata.ExplicitPin{first.DataPinOverrides[1], first.DataPinOverrides[0]}
	_, same, err := reordered.Canonical()
	if err != nil || same != hash {
		t.Fatalf("same selected request has different semantic hash: %q %q %v", hash, same, err)
	}
	changed := first
	changed.DataPinOverrides = append([]durabledata.ExplicitPin(nil), first.DataPinOverrides...)
	changed.DataPinOverrides[0].VersionID = durabledata.VersionID("resource-version-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	_, different, err := changed.Canonical()
	if err != nil || different == hash {
		t.Fatalf("changed pin reused semantic hash: %q %q %v", hash, different, err)
	}
}

func TestForkOperationRejectsMissingIdentityAndDuplicatePins(t *testing.T) {
	request := ForkOperationRequest{OperationID: uuid.NewString(), Actor: "actor", TransportHash: "hash", SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(), TargetBundleHash: "bundle"}
	for _, change := range []func(*ForkOperationRequest){
		func(r *ForkOperationRequest) { r.OperationID = "" },
		func(r *ForkOperationRequest) { r.Actor = "" },
		func(r *ForkOperationRequest) { r.TransportHash = "" },
		func(r *ForkOperationRequest) { r.SourceRunID = "foreign" },
		func(r *ForkOperationRequest) {
			pin := durabledata.ExplicitPin{Declaration: durabledata.DeclarationRef{FlowPath: "a", EventName: "a/x"}, VersionID: durabledata.VersionID("resource-version-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}
			r.DataPinOverrides = []durabledata.ExplicitPin{pin, pin}
		},
	} {
		broken := request
		change(&broken)
		if _, _, err := broken.Canonical(); err == nil {
			t.Fatalf("invalid fork operation accepted: %+v", broken)
		}
	}
}

func TestForkOperationKeepsEventlessSelectorDistinctFromEventFork(t *testing.T) {
	request := ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "bearer:operator", TransportHash: "exact-wire-request",
		SourceRunID: uuid.NewString(), TargetBundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContractSelection: RunForkContractSelection{Mode: RunForkContractSelectionModeSelectedContracts},
	}
	_, eventlessHash, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	request.ForkEventID = uuid.NewString()
	_, eventHash, err := request.Canonical()
	if err != nil || eventHash == eventlessHash {
		t.Fatalf("event fork aliased eventless request: event=%q eventless=%q err=%v", eventHash, eventlessHash, err)
	}
	request.ForkEventID = "not-a-uuid"
	if _, _, err := request.Canonical(); err == nil {
		t.Fatal("invalid optional event selector was admitted")
	}
}

func TestForkOperationResolvedDeploymentPointDoesNotRehashRequest(t *testing.T) {
	request := ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "bearer:operator", TransportHash: "exact-wire-request",
		SourceRunID: uuid.NewString(), TargetBundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContractSelection: RunForkContractSelection{Mode: RunForkContractSelectionModeSelectedContracts},
	}
	_, requestHash, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	request.ResolvedPoint = &RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 4}
	_, resolvedHash, err := request.Canonical()
	if err != nil || resolvedHash != requestHash {
		t.Fatalf("resolution changed caller request meaning: before=%q after=%q err=%v", requestHash, resolvedHash, err)
	}
	request.ForkEventID = uuid.NewString()
	if _, _, err := request.Canonical(); err == nil {
		t.Fatal("event selector accepted a deployment revision point")
	}
}

func TestForkOperationRecordRequiresExactResolvedPoint(t *testing.T) {
	request := ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "bearer:operator", TransportHash: "exact-wire-request",
		SourceRunID: uuid.NewString(), TargetBundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContractSelection: RunForkContractSelection{Mode: RunForkContractSelectionModeSelectedContracts},
	}
	_, hash, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	record := ForkOperationRecord{
		Request: request, SemanticHash: hash, ForkRunID: uuid.NewString(), BindingID: uuid.NewString(), Status: ForkOperationMaterialized,
	}
	if err := record.Validate(); err == nil {
		t.Fatal("materialized operation without selected revision was accepted")
	}
	point := RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 4}
	record.Request.ResolvedPoint = &point
	if err := record.Validate(); err != nil {
		t.Fatalf("exact deployment revision refused: %v", err)
	}
	record.Status = ForkOperationActivated
	record.Result = &ForkOperationResult{
		SourceRunID: request.SourceRunID, SourceRunStatus: RunForkSourceFrozenStatus, SourceFrozen: true,
		ForkRunID: record.ForkRunID, ForkPoint: point, ForkRunStatus: RunForkActivatedStatus,
		BundleHash: request.TargetBundleHash,
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("activated deployment revision refused: %v", err)
	}
	pin := durabledata.Pin{
		RunID: record.ForkRunID, RunState: RunForkActivatedStatus,
		Declaration:  durabledata.DeclarationRef{FlowPath: ".", EventName: "account.registered"},
		SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		VersionID:    durabledata.VersionID("resource-version-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		Selection:    "fork_override",
	}
	record.Result.DataPins = []durabledata.Pin{pin}
	if err := record.Validate(); err != nil {
		t.Fatalf("exact child pin refused: %v", err)
	}
	record.Result.DataPins = []durabledata.Pin{pin, pin}
	if err := record.Validate(); err == nil {
		t.Fatal("duplicate declaration in durable fork result was accepted")
	}
	record.Result.DataPins = []durabledata.Pin{pin}
	record.Result.DataPins[0].RunID = request.SourceRunID
	if err := record.Validate(); err == nil {
		t.Fatal("source-run pin in durable child result was accepted")
	}
	record.Result.DataPins = nil
	record.Result.ForkPoint.Revision++
	if err := record.Validate(); err == nil {
		t.Fatal("activated operation changed its selected revision")
	}
}
