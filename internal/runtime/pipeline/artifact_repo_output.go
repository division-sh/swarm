package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func artifactRepoOutputSchemaError(err error) error {
	return failures.Wrap(failures.ClassSchemaInvalid, "artifact_repo_output_schema_invalid", "artifact-repo", "validate_output_state", nil, err)
}

// Output mappings write receiving entity state, independently of result-event schemas.
func (pc *PipelineCoordinator) validateArtifactRepoOutputContract(execCtx engine.ExecutionContext, spec *contracts.ArtifactRepoSpec, repoID, requestID, sourceEventID string) error {
	contract, found := entityruntime.ResolveForFlow(pc.SemanticSource(), execCtx.Request.Node.FlowPath())
	invalid := artifactRepoOutputSchemaError
	if !found {
		return invalid(fmt.Errorf("artifact output requires the receiving entity contract"))
	}
	seen := map[string]bool{}
	for _, output := range []struct {
		name  string
		field string
		value any
	}{
		{"repo_url", spec.Output.RepoURL, artifactRepoPublicScheme + repoID},
		{"current_ref", spec.Output.CurrentRef, ""},
		{"file_manifest", spec.Output.FileManifest, nil},
		{"status", spec.Output.Status, "committed"},
		{"failure", spec.Output.Failure, map[string]any{}},
		{"last_request_id", spec.Output.LastRequestID, requestID},
		{"last_source_event_id", spec.Output.LastSourceEventID, sourceEventID},
	} {
		field := strings.TrimSpace(output.field)
		if field == "" || field != output.field || seen[field] {
			return invalid(fmt.Errorf("artifact output.%s requires a distinct entity field", output.name))
		}
		seen[field] = true
		if _, ok := contract.Entity.Fields[field]; !ok {
			return invalid(fmt.Errorf("artifact output.%s targets undeclared entity field %q", output.name, field))
		}
		decl, err := entityruntime.ResolveFieldPath(contract, field)
		if err != nil {
			return invalid(err)
		}
		resolved, err := (contracts.CatalogTypeReference{Type: decl.Type, Catalog: contract.Types}).Resolve()
		if err != nil {
			return invalid(err)
		}
		switch output.name {
		case "current_ref":
			if err := artifactRepoAdmitProviderRef(contract, field); err != nil {
				return invalid(err)
			}
		case "failure":
			// Both {} and an arbitrary canonical failure envelope must fit this field.
			if resolved.Kind != contracts.CatalogTypeDynamic || decl.LeafKind != "object" || !decl.Refinements.Empty() || entityruntime.FieldPathParticipatesInEquality(contract, field) {
				return invalid(fmt.Errorf("artifact output.failure requires an unrestricted JSON object"))
			}
		case "file_manifest":
			if entityruntime.FieldPathParticipatesInEquality(contract, field) {
				return invalid(fmt.Errorf("artifact provider-dependent manifest cannot participate in root equality"))
			}
			if resolved.Kind == contracts.CatalogTypeObject {
				if err := artifactRepoAdmitProviderRef(contract, field+".ref"); err != nil {
					return invalid(err)
				}
			} else if resolved.Kind != contracts.CatalogTypeDynamic || decl.LeafKind != "object" {
				return invalid(fmt.Errorf("artifact output.file_manifest requires an object"))
			}
			// Admit the actual prepared manifest before provider access below.
			continue
		case "status":
			if _, err := entityruntime.NormalizeFieldValue(contract, field, "failed"); err != nil {
				return invalid(err)
			}
		}
		if _, err := entityruntime.NormalizeFieldValue(contract, field, output.value); err != nil {
			return invalid(err)
		}
	}
	// A complete same-event result performs no output mutation. Missing any
	// mapped output instead requires reconstruction and the candidate checks.
	current := execCtx.Request.State.StateCarrier.Fields
	if asString(current[spec.Output.LastSourceEventID]) == sourceEventID && artifactRepoOutputsComplete(current, spec) {
		_, err := entityruntime.NormalizeState(contract, current)
		if err != nil {
			return invalid(err)
		}
		return nil
	}
	for _, field := range []string{spec.Output.CurrentRef, spec.Output.FileManifest} {
		if _, present := current[field]; present && contract.Entity.Fields[field].Immutable {
			return invalid(fmt.Errorf("artifact provider-dependent output %s cannot replace an assigned immutable field", field))
		}
	}
	if strings.TrimSpace(spec.FailureEvent) != "" {
		if _, present := current[spec.Output.Failure]; present && contract.Entity.Fields[spec.Output.Failure].Immutable {
			return invalid(fmt.Errorf("artifact failure output cannot replace an assigned immutable field"))
		}
		if err := validateArtifactRepoOutputCandidate(contract, current, map[string]any{
			spec.Output.Status: "failed", spec.Output.Failure: map[string]any{},
			spec.Output.LastRequestID: requestID, spec.Output.LastSourceEventID: sourceEventID,
		}); err != nil {
			return err
		}
	}
	// Known values are checked together, preserving valid paired equality.
	// Unknown refs/failure data have unrestricted, equality-free domains above.
	return validateArtifactRepoOutputCandidate(contract, current, map[string]any{
		spec.Output.RepoURL: artifactRepoPublicScheme + repoID,
		spec.Output.Status:  "committed", spec.Output.Failure: map[string]any{},
		spec.Output.LastRequestID: requestID, spec.Output.LastSourceEventID: sourceEventID,
	})
}

func validateArtifactRepoOutputCandidate(contract entityruntime.Contract, current, fields map[string]any) error {
	_, err := entityruntime.ApplyMutations(contract, current, artifactRepoResultMutations(fields))
	if err != nil {
		return artifactRepoOutputSchemaError(err)
	}
	return nil
}

func artifactRepoAdmitProviderRef(contract entityruntime.Contract, field string) error {
	decl, err := entityruntime.ResolveFieldPath(contract, field)
	if err != nil {
		return err
	}
	resolved, err := (contracts.CatalogTypeReference{Type: decl.Type, Catalog: contract.Types}).Resolve()
	if err != nil {
		return err
	}
	// The provider has not produced a ref yet. Do not use a sample hash to
	// approve an enum, equality or refinement that can reject the real hash later.
	if resolved.Kind != contracts.CatalogTypeText || resolved.Name != "" || !decl.Refinements.Empty() || entityruntime.FieldPathParticipatesInEquality(contract, field) {
		return fmt.Errorf("artifact provider ref field %s requires unrestricted text", field)
	}
	_, err = entityruntime.NormalizeFieldValue(contract, field, "")
	return err
}

func (pc *PipelineCoordinator) validateArtifactRepoManifestOutput(execCtx engine.ExecutionContext, spec *contracts.ArtifactRepoSpec, manifest map[string]any) error {
	contract, found := entityruntime.ResolveForFlow(pc.SemanticSource(), execCtx.Request.Node.FlowPath())
	if !found {
		return artifactRepoOutputSchemaError(fmt.Errorf("artifact output requires the receiving entity contract"))
	}
	// Only ref is not yet known, and its unrestricted text domain was admitted
	// above. All other manifest values are the actual prepared provider inputs.
	return validateArtifactRepoOutputCandidate(contract, execCtx.Request.State.StateCarrier.Fields, map[string]any{
		spec.Output.RepoURL: manifest["repo_url"], spec.Output.CurrentRef: manifest["ref"],
		spec.Output.FileManifest: manifest, spec.Output.Status: "committed", spec.Output.Failure: map[string]any{},
		spec.Output.LastRequestID: manifest["request_id"], spec.Output.LastSourceEventID: manifest["source_event_id"],
	})
}
