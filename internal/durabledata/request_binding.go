package durabledata

import (
	"encoding/hex"
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

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
	if parsed, err := uuid.Parse(b.RunID); err != nil || parsed == uuid.Nil || parsed.String() != b.RunID {
		return fmt.Errorf("request binding has invalid run_id")
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(b.BundleHash); err != nil {
		return err
	}
	if b.EventID != "" {
		if parsed, err := uuid.Parse(b.EventID); err != nil || parsed == uuid.Nil || parsed.String() != b.EventID {
			return fmt.Errorf("request binding has invalid event_id")
		}
	}
	const prefix = "resource-run-creation-request-v1:sha256:"
	digest := strings.TrimPrefix(b.RequestHash, prefix)
	if !strings.HasPrefix(b.RequestHash, prefix) || len(digest) != 64 {
		return fmt.Errorf("request binding has invalid request_hash")
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || hex.EncodeToString(decoded) != digest {
		return fmt.Errorf("request binding has invalid request_hash")
	}
	if len(b.Imports)+len(b.Pins) > MaxDataDeclarationsPerBundle {
		return fmt.Errorf("request binding exceeds declaration bound")
	}
	if b.EventID == "" && len(b.Imports)+len(b.Pins) == 0 {
		return fmt.Errorf("request binding has no initiation")
	}
	selected := make(map[string]struct{}, len(b.Imports)+len(b.Pins))
	invocations := make(map[string]struct{}, len(b.Imports))
	for index, item := range b.Imports {
		if parsed, err := uuid.Parse(item.SourceInvocationID); err != nil || parsed == uuid.Nil || parsed.String() != item.SourceInvocationID {
			return fmt.Errorf("request binding has invalid source_invocation_id")
		}
		if err := item.Declaration.Validate(); err != nil {
			return err
		}
		if err := item.SchemaDigest.Validate(); err != nil {
			return err
		}
		if err := item.ExpectedHead.Validate(); err != nil {
			return err
		}
		if index > 0 && CompareDeclarationRef(b.Imports[index-1].Declaration, item.Declaration) >= 0 {
			return fmt.Errorf("request imports are not declaration-sorted")
		}
		if _, duplicate := invocations[item.SourceInvocationID]; duplicate {
			return fmt.Errorf("request imports repeat source_invocation_id")
		}
		invocations[item.SourceInvocationID] = struct{}{}
		selected[item.Declaration.Key()] = struct{}{}
	}
	for index, item := range b.Pins {
		if err := item.validate(); err != nil {
			return err
		}
		if index > 0 && CompareDeclarationRef(b.Pins[index-1].Declaration, item.Declaration) >= 0 {
			return fmt.Errorf("request pins are not declaration-sorted")
		}
		if _, duplicate := selected[item.Declaration.Key()]; duplicate {
			return fmt.Errorf("request pin overlaps another selection")
		}
		selected[item.Declaration.Key()] = struct{}{}
	}
	return nil
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
	if b.RunID != record.Summary.RunID || b.BundleHash != record.Summary.BundleHash || len(b.Imports) != record.Summary.ImportCount {
		return fmt.Errorf("request binding contradicts operation summary")
	}
	if record.Summary.Outcome != "created" && len(record.Evidence.ChildEvaluations) != len(b.Imports) {
		return fmt.Errorf("rejected child count contradicts request imports")
	}
	if record.Summary.Outcome == "created" {
		if b.EventID != record.Summary.EventID || len(b.Imports)+len(b.Pins) != record.Summary.PinCount {
			return fmt.Errorf("request binding contradicts created run")
		}
	} else {
		for index, item := range b.Imports {
			child := record.Evidence.ChildEvaluations[index]
			if child.SourceInvocationID != item.SourceInvocationID || child.BundleHash != b.BundleHash ||
				child.Declaration != item.Declaration || child.SchemaDigest != item.SchemaDigest || child.ExpectedHead != item.ExpectedHead {
				return fmt.Errorf("request import contradicts rejected child")
			}
		}
	}
	if record.Summary.Outcome == "created" {
		imports := make(map[string]RunCreationRequestImport, len(b.Imports))
		selected := make(map[string]struct{}, len(b.Imports)+len(b.Pins))
		for _, item := range b.Imports {
			imports[item.Declaration.Key()] = item
			selected[item.Declaration.Key()] = struct{}{}
		}
		for _, item := range b.Pins {
			selected[item.Declaration.Key()] = struct{}{}
		}
		for _, item := range record.Evidence.RunBinding {
			if item.Kind == "import" {
				requested, found := imports[item.Import.Declaration.Key()]
				if !found || requested.SourceInvocationID != item.Import.SourceInvocationID ||
					requested.SchemaDigest != item.Import.SchemaDigest || requested.ExpectedHead != item.Import.ExpectedHead {
					return fmt.Errorf("bound import contradicts request")
				}
				continue
			}
			pin := item.Pin
			if _, found := selected[pin.Declaration.Key()]; !found {
				return fmt.Errorf("bound pin is absent from request")
			}
			if _, imported := imports[pin.Declaration.Key()]; imported != (pin.Selection == "fused_import") {
				return fmt.Errorf("bound pin selection contradicts request")
			}
			if pin.Selection == "explicit" {
				found := false
				for _, requested := range b.Pins {
					found = found || requested.Declaration == pin.Declaration && requested.VersionID == pin.VersionID
				}
				if !found {
					return fmt.Errorf("bound explicit pin contradicts request")
				}
			}
		}
	} else if target := record.Summary.Rejection.Declaration; target != nil {
		found := false
		for _, item := range b.Pins {
			found = found || item.Declaration == *target && item.VersionID == record.Summary.Rejection.VersionID
		}
		if !found {
			return fmt.Errorf("rejection target is absent from requested pins")
		}
	}
	return nil
}
