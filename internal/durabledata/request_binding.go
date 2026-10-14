package durabledata

import "fmt"

// RunCreationRequestImport contains only the retained identity and CAS facts
// needed to reconstruct one originally requested fused import.
type RunCreationRequestImport struct {
	SourceInvocationID string         `json:"source_invocation_id"`
	Declaration        DeclarationRef `json:"declaration"`
	SchemaDigest       SchemaDigest   `json:"schema_digest"`
	ExpectedHead       ExpectedHead   `json:"expected_head"`
}

// RunCreationRequestBinding is a bounded, payload-free view of the canonical
// request retained by a run-creation operation, including rejected requests.
type RunCreationRequestBinding struct {
	RunID       string                     `json:"run_id"`
	BundleHash  string                     `json:"bundle_hash"`
	EventID     string                     `json:"event_id,omitempty"`
	RequestHash string                     `json:"request_hash"`
	Imports     []RunCreationRequestImport `json:"imports"`
	Pins        []ExplicitPin              `json:"pins"`
}

// BindRunCreationRequest must be called only with the selected store's
// validated retained command and receipt. It never derives missing pins from
// a rejected operation's partial public evidence.
func BindRunCreationRequest(command RunCreationCommand, record RunCreationOperationRecord) (RunCreationRequestBinding, error) {
	hash, _, canonical, err := command.RequestHash()
	if err != nil {
		return RunCreationRequestBinding{}, err
	}
	if err := record.ValidateForCommand(canonical); err != nil {
		return RunCreationRequestBinding{}, err
	}
	binding := RunCreationRequestBinding{
		RunID: canonical.RunID, BundleHash: canonical.BundleHash, EventID: canonical.EventID,
		RequestHash: hash, Imports: make([]RunCreationRequestImport, 0, len(canonical.Data.Imports)),
		Pins: append([]ExplicitPin{}, canonical.Data.Pins...),
	}
	createdImports := make(map[string]SourceOperationSummary, len(canonical.Data.Imports))
	if record.Summary.Outcome == "created" {
		for _, item := range record.Evidence.RunBinding {
			if item.Kind == "import" && item.Import != nil {
				createdImports[item.Import.Declaration.Key()] = *item.Import
			}
		}
	} else if len(record.Evidence.ChildEvaluations) != len(canonical.Data.Imports) {
		return RunCreationRequestBinding{}, fmt.Errorf("rejected child count contradicts requested imports")
	}
	for index, item := range canonical.Data.Imports {
		var schemaDigest SchemaDigest
		if record.Summary.Outcome == "created" {
			child, found := createdImports[item.Declaration.Key()]
			if !found || child.SourceInvocationID != item.SourceInvocationID {
				return RunCreationRequestBinding{}, fmt.Errorf("created import is absent from run binding")
			}
			schemaDigest = child.SchemaDigest
		} else {
			child := record.Evidence.ChildEvaluations[index]
			if child.SourceInvocationID != item.SourceInvocationID || child.Declaration != item.Declaration {
				return RunCreationRequestBinding{}, fmt.Errorf("rejected child contradicts requested import")
			}
			schemaDigest = child.SchemaDigest
		}
		binding.Imports = append(binding.Imports, RunCreationRequestImport{
			SourceInvocationID: item.SourceInvocationID, Declaration: item.Declaration,
			SchemaDigest: schemaDigest, ExpectedHead: item.ExpectedHead,
		})
	}
	if err := binding.ValidateForRecord(record); err != nil {
		return RunCreationRequestBinding{}, err
	}
	return binding, nil
}

// Validate checks the standalone public DTO before a caller uses it for
// reconstruction. The retained request hash is still the final equality owner.
func (b RunCreationRequestBinding) Validate() error {
	if err := b.validateIdentity(); err != nil {
		return err
	}
	return b.validateSelections()
}

// ValidateForRecord checks the standalone DTO and its relationships to the
// selected-store validated operation without exposing retained payloads.
func (b RunCreationRequestBinding) ValidateForRecord(record RunCreationOperationRecord) error {
	if err := b.Validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if err := b.validateRecordSummary(record.Summary); err != nil {
		return err
	}
	if record.Summary.Outcome == "created" {
		return b.validateCreatedRecord(record)
	}
	return b.validateRejectedRecord(record)
}
