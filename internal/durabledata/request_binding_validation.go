package durabledata

import (
	"encoding/hex"
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

func (b RunCreationRequestBinding) validateIdentity() error {
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
	return nil
}

func (b RunCreationRequestBinding) validateSelections() error {
	if len(b.Imports)+len(b.Pins) > MaxDataDeclarationsPerBundle {
		return fmt.Errorf("request binding exceeds declaration bound")
	}
	if b.EventID == "" && len(b.Imports)+len(b.Pins) == 0 {
		return fmt.Errorf("request binding has no initiation")
	}
	selected := make(map[string]struct{}, len(b.Imports)+len(b.Pins))
	if err := b.validateImports(selected); err != nil {
		return err
	}
	return b.validatePins(selected)
}

func (b RunCreationRequestBinding) validateImports(selected map[string]struct{}) error {
	invocations := make(map[string]struct{}, len(b.Imports))
	for index, item := range b.Imports {
		if err := item.validate(); err != nil {
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
	return nil
}

func (i RunCreationRequestImport) validate() error {
	if parsed, err := uuid.Parse(i.SourceInvocationID); err != nil || parsed == uuid.Nil || parsed.String() != i.SourceInvocationID {
		return fmt.Errorf("request binding has invalid source_invocation_id")
	}
	if err := i.Declaration.Validate(); err != nil {
		return err
	}
	if err := i.SchemaDigest.Validate(); err != nil {
		return err
	}
	return i.ExpectedHead.Validate()
}

func (b RunCreationRequestBinding) validatePins(selected map[string]struct{}) error {
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

func (b RunCreationRequestBinding) validateRecordSummary(summary RunCreationOperationSummary) error {
	if b.RunID != summary.RunID || b.BundleHash != summary.BundleHash || len(b.Imports) != summary.ImportCount {
		return fmt.Errorf("request binding contradicts operation summary")
	}
	return nil
}

func (b RunCreationRequestBinding) validateCreatedRecord(record RunCreationOperationRecord) error {
	if b.EventID != record.Summary.EventID || len(b.Imports)+len(b.Pins) != record.Summary.PinCount {
		return fmt.Errorf("request binding contradicts created run")
	}
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
			if err := validateRequestedBoundImport(item.Import, imports); err != nil {
				return err
			}
			continue
		}
		if err := b.validateBoundPin(item.Pin, imports, selected); err != nil {
			return err
		}
	}
	return nil
}

func validateRequestedBoundImport(item *SourceOperationSummary, imports map[string]RunCreationRequestImport) error {
	requested, found := imports[item.Declaration.Key()]
	if !found || requested.SourceInvocationID != item.SourceInvocationID ||
		requested.SchemaDigest != item.SchemaDigest || requested.ExpectedHead != item.ExpectedHead {
		return fmt.Errorf("bound import contradicts request")
	}
	return nil
}

func (b RunCreationRequestBinding) validateBoundPin(pin *Pin, imports map[string]RunCreationRequestImport, selected map[string]struct{}) error {
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
	return nil
}

func (b RunCreationRequestBinding) validateRejectedRecord(record RunCreationOperationRecord) error {
	if len(record.Evidence.ChildEvaluations) != len(b.Imports) {
		return fmt.Errorf("rejected child count contradicts request imports")
	}
	for index, item := range b.Imports {
		child := record.Evidence.ChildEvaluations[index]
		if child.SourceInvocationID != item.SourceInvocationID || child.BundleHash != b.BundleHash ||
			child.Declaration != item.Declaration || child.SchemaDigest != item.SchemaDigest || child.ExpectedHead != item.ExpectedHead {
			return fmt.Errorf("request import contradicts rejected child")
		}
	}
	return b.validateRejectionTarget(record.Summary.Rejection)
}

func (b RunCreationRequestBinding) validateRejectionTarget(rejection RunCreationRejection) error {
	if target := rejection.Declaration; target != nil {
		found := false
		for _, item := range b.Pins {
			found = found || item.Declaration == *target && item.VersionID == rejection.VersionID
		}
		if !found {
			return fmt.Errorf("rejection target is absent from requested pins")
		}
	}
	return nil
}
