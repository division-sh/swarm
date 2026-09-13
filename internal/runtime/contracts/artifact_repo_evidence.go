package contracts

import "strings"

// ArtifactRepoResultPublication separates action-owned payload fields from
// authored additions. Diagnostic labels do not confer runtime field ownership.
type ArtifactRepoResultPublication struct {
	label         string
	emit          EmitSpec
	runtimeFields []string
}

func (p ArtifactRepoResultPublication) Label() string      { return p.label }
func (p ArtifactRepoResultPublication) EmitSpec() EmitSpec { return cloneEmitSpec(p.emit) }
func (p ArtifactRepoResultPublication) RuntimePayloadFields() []string {
	return append([]string(nil), p.runtimeFields...)
}

func (s ArtifactRepoSpec) ResultPublications() []ArtifactRepoResultPublication {
	fields := []string{"repo_id", "namespace", "request_id", "source_event_id", "provenance"}
	if !s.PartitionKey.IsZero() {
		fields = append(fields, "partition_key")
	}
	if !s.DisplaySlug.IsZero() {
		fields = append(fields, "display_slug")
	}
	return []ArtifactRepoResultPublication{
		{label: "success", emit: cloneEmitSpec(EmitSpec{Event: s.SuccessEvent, Fields: s.SuccessPayload}),
			runtimeFields: append(append([]string(nil), fields...), "repo_url", "current_ref", "file_manifest")},
		{label: "failure", emit: cloneEmitSpec(EmitSpec{Event: s.FailureEvent, Fields: s.FailurePayload}),
			runtimeFields: append(append([]string(nil), fields...), "failure")},
	}
}

func ArtifactRepoResultPayloadFieldReserved(field string) bool {
	switch strings.TrimSpace(field) {
	case "repo_id", "namespace", "partition_key", "display_slug", "request_id", "source_event_id", "repo_url", "current_ref", "file_manifest", "failure", "provenance":
		return true
	default:
		return false
	}
}

type ArtifactRepoOutputField struct {
	Name   string
	Target string
}

func (o ArtifactRepoOutputSpec) Fields() []ArtifactRepoOutputField {
	return []ArtifactRepoOutputField{
		{"repo_url", o.RepoURL}, {"current_ref", o.CurrentRef}, {"file_manifest", o.FileManifest},
		{"status", o.Status}, {"failure", o.Failure}, {"last_request_id", o.LastRequestID},
		{"last_source_event_id", o.LastSourceEventID},
	}
}
