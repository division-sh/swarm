package contracts

import (
	"reflect"
	"slices"
	"testing"
)

func TestArtifactRepoPublicationEvidence(t *testing.T) {
	for _, optional := range []bool{false, true} {
		spec := ArtifactRepoSpec{
			SuccessEvent: "commit.ok", FailureEvent: "commit.failed",
			SuccessPayload: map[string]ExpressionValue{"extra": LiteralExpression("success")},
			FailurePayload: map[string]ExpressionValue{"extra": LiteralExpression("failure")},
		}
		if optional {
			spec.PartitionKey, spec.DisplaySlug = LiteralExpression("partition"), LiteralExpression("display")
		}
		publications := spec.ResultPublications()
		if len(publications) != 2 {
			t.Fatalf("publications = %#v", publications)
		}
		for i, publication := range publications {
			want := []string{"repo_id", "namespace", "request_id", "source_event_id", "provenance"}
			if optional {
				want = append(want, "partition_key", "display_slug")
			}
			label := "success"
			if i == 0 {
				want = append(want, "repo_url", "current_ref", "file_manifest")
			} else {
				label = "failure"
				want = append(want, "failure")
			}
			if publication.Label() != label || !reflect.DeepEqual(publication.RuntimePayloadFields(), want) {
				t.Fatalf("%s optional=%v: fields = %v, want %v", label, optional, publication.RuntimePayloadFields(), want)
			}
			fields := publication.RuntimePayloadFields()
			fields[0] = "forged"
			emit := publication.EmitSpec()
			emit.Fields["extra"] = LiteralExpression("forged")
			if !reflect.DeepEqual(publication.RuntimePayloadFields(), want) || publication.EmitSpec().Fields["extra"].Literal != label {
				t.Fatal("reader mutated retained publication evidence")
			}
		}
		spec.SuccessPayload["extra"] = LiteralExpression("changed source")
		if publications[0].EmitSpec().Fields["extra"].Literal != "success" {
			t.Fatal("source mutation changed retained publication evidence")
		}
	}
}

func TestArtifactRepoEmitSitesKeepRuntimeOwnershipSeparate(t *testing.T) {
	action := ActionSpec{ID: "artifact_repo_commit", ArtifactRepo: &ArtifactRepoSpec{
		SuccessEvent: "commit.ok", FailureEvent: "commit.failed",
		SuccessPayload: map[string]ExpressionValue{"extra": LiteralExpression("ready")},
	}}
	for _, handler := range []SystemNodeEventHandler{
		{Action: action},
		{Rules: []HandlerRuleEntry{{Action: action}}},
	} {
		sites := HandlerDeclarativeEmitSites(handler)
		if len(sites) != 2 {
			t.Fatalf("artifact sites = %#v", sites)
		}
		for _, site := range sites {
			if !slices.Contains(site.RuntimePayloadFields(), "request_id") {
				t.Fatalf("site lost runtime evidence: %#v", site)
			}
			if _, exists := site.Spec.Fields["request_id"]; exists {
				t.Fatal("runtime field became an authored expression")
			}
			fields := site.RuntimePayloadFields()
			fields[0] = "forged"
			if slices.Contains(site.RuntimePayloadFields(), "forged") {
				t.Fatal("runtime field evidence aliases reader output")
			}
		}
	}
	forged := HandlerDeclarativeEmitSite{Source: "handler.action.success", Spec: EmitSpec{Event: "commit.ok"}}
	if len(forged.RuntimePayloadFields()) != 0 {
		t.Fatal("diagnostic label conferred payload authority")
	}
}

func TestArtifactRepoOutputEvidenceAndReservedFields(t *testing.T) {
	output := ArtifactRepoOutputSpec{"url", "ref", "manifest", "state", "error", "request", "source"}
	want := []ArtifactRepoOutputField{
		{"repo_url", "url"}, {"current_ref", "ref"}, {"file_manifest", "manifest"},
		{"status", "state"}, {"failure", "error"}, {"last_request_id", "request"}, {"last_source_event_id", "source"},
	}
	if !reflect.DeepEqual(output.Fields(), want) {
		t.Fatalf("mapped fields = %#v, want %#v", output.Fields(), want)
	}
	// A new output declaration must be deliberately added to the semantic census.
	if reflect.TypeOf(output).NumField() != len(want) {
		t.Fatal("artifact output fields escaped the evidence census")
	}
	for _, field := range []string{"repo_id", "namespace", "partition_key", "display_slug", "request_id", "source_event_id", "repo_url", "current_ref", "file_manifest", "failure", "provenance"} {
		if !ArtifactRepoResultPayloadFieldReserved(field) || !ArtifactRepoResultPayloadFieldReserved(" "+field+" ") {
			t.Fatalf("runtime field %s permits an authored override", field)
		}
	}
	if ArtifactRepoResultPayloadFieldReserved("result_kind") {
		t.Fatal("authored addition was reserved")
	}
}
