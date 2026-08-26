package durabledata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func FirstEvidencePage[T any](all []T) PageResult[T] {
	items := make([]T, 0, min(len(all), MaxPublicPageItems))
	encodedItems := 0
	for _, item := range all {
		raw, _ := json.Marshal(item)
		encodedPageBytes := 2 + encodedItems + len(raw) + len(items)
		if len(items) >= MaxPublicPageItems || encodedPageBytes > MaxPublicPageBytes {
			break
		}
		items = append(items, item)
		encodedItems += len(raw)
	}
	raw, _ := json.Marshal(items)
	continuation := EndContinuation()
	if len(items) < len(all) {
		continuation = PageContinuation{State: "more", Cursor: fmt.Sprintf("%d", len(items))}
	}
	return PageResult[T]{Items: items, ItemCount: len(items), EncodedItemsBytes: len(raw), Continuation: continuation}
}

func validateCanonicalUUID(raw, field string) error {
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil || parsed.String() != raw {
		return fmt.Errorf("%s must be one canonical non-zero UUID", field)
	}
	return nil
}

func validateCompletedAt(completedAt time.Time) error {
	if completedAt.IsZero() || completedAt.Location() != time.UTC || completedAt != completedAt.Truncate(time.Microsecond) {
		return fmt.Errorf("completed_at must be one non-zero UTC microsecond timestamp")
	}
	return nil
}

func validateCanonicalRunState(raw, field string) error {
	state, err := runtimerunlifecycle.ParseState(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	if raw != string(state) {
		return fmt.Errorf("%s must use canonical run lifecycle spelling", field)
	}
	return nil
}

func (h HeadResult) Validate() error {
	if err := h.Before.Validate(); err != nil {
		return fmt.Errorf("head before: %w", err)
	}
	if err := h.After.Validate(); err != nil {
		return fmt.Errorf("head after: %w", err)
	}
	changed := !h.Before.Equal(h.After)
	if h.Changed != changed {
		return fmt.Errorf("head changed contradicts before and after")
	}
	if changed && h.Revision == 0 {
		return fmt.Errorf("changed head requires a positive revision")
	}
	return nil
}

func validateSourceOperationResult(r SourceOperationResult) error {
	if err := validateCanonicalUUID(r.SourceInvocationID, "source_invocation_id"); err != nil {
		return err
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(r.BundleHash); err != nil {
		return err
	}
	if err := r.SchemaDigest.Validate(); err != nil {
		return err
	}
	if err := r.Declaration.Validate(); err != nil {
		return err
	}
	if err := r.ExpectedHead.Validate(); err != nil {
		return fmt.Errorf("expected head: %w", err)
	}
	if err := r.ObservedHead.Validate(); err != nil {
		return fmt.Errorf("observed head: %w", err)
	}
	if err := r.ValidateCandidateState(); err != nil {
		return err
	}
	if r.Candidate.Manifest != nil && (r.Candidate.Manifest.Declaration != r.Declaration || r.Candidate.Manifest.SchemaDigest != r.SchemaDigest) {
		return fmt.Errorf("candidate manifest contradicts declaration or schema digest")
	}
	if err := r.Head.Validate(); err != nil {
		return err
	}
	if !r.Head.Before.Equal(r.ObservedHead) {
		return fmt.Errorf("head before contradicts observed head")
	}
	if err := r.Delta.Validate(); err != nil {
		return err
	}
	if err := r.Defects.Validate(); err != nil {
		return fmt.Errorf("source defects: %w", err)
	}
	for _, defect := range r.Defects.Items {
		if err := defect.Validate(); err != nil {
			return err
		}
	}
	if err := validateCompletedAt(r.CompletedAt); err != nil {
		return err
	}

	switch r.Outcome {
	case "validation_rejected":
		if r.Defects.ItemCount == 0 || r.Delta.State != "not_computed" || r.Delta.Reason != "validation_rejected" ||
			!r.Head.Before.Equal(r.Head.After) {
			return fmt.Errorf("validation_rejected source result has contradictory defects, delta, or head")
		}
	case "head_conflict":
		if r.Defects.ItemCount != 0 || r.ExpectedHead.Equal(r.ObservedHead) || r.Delta.State != "not_computed" ||
			r.Delta.Reason != "head_conflict" || !r.Head.Before.Equal(r.Head.After) {
			return fmt.Errorf("head_conflict source result has contradictory head, delta, or defects")
		}
	case "accepted":
		if r.Defects.ItemCount != 0 || !r.ExpectedHead.Equal(r.ObservedHead) || r.Delta.State == "not_computed" ||
			r.Delta.Against == nil || !r.Delta.Against.Equal(r.ObservedHead) {
			return fmt.Errorf("accepted source result has contradictory head, delta, or defects")
		}
		if r.Operation == "check" {
			if !r.Head.Before.Equal(r.Head.After) {
				return fmt.Errorf("accepted check must not change head")
			}
		} else if r.Head.After != VersionHead(r.Candidate.VersionID) {
			return fmt.Errorf("accepted import head must select candidate version")
		}
	default:
		return fmt.Errorf("source outcome is unsupported")
	}
	return nil
}

func (d ValidationDefect) Validate() error {
	if strings.TrimSpace(d.Code) == "" || strings.TrimSpace(d.Message) == "" {
		return fmt.Errorf("validation defect requires code and message")
	}
	return nil
}

func validateDeltaEvidence(evidence SourceEvidence, delta DeltaResult) error {
	groups := [][]DeltaKey{evidence.DeltaAdded, evidence.DeltaRemoved, evidence.DeltaChanged}
	seen := map[BusinessKey]struct{}{}
	for _, group := range groups {
		for index, item := range group {
			if err := item.Key.Validate(); err != nil {
				return err
			}
			if index > 0 && strings.Compare(string(group[index-1].Key), string(item.Key)) >= 0 {
				return fmt.Errorf("delta evidence keys must be strictly sorted")
			}
			if _, duplicate := seen[item.Key]; duplicate {
				return fmt.Errorf("delta evidence key appears in multiple classes")
			}
			seen[item.Key] = struct{}{}
		}
	}
	if delta.State != "computed" {
		if len(seen) != 0 {
			return fmt.Errorf("uncomputed delta forbids key evidence")
		}
		return nil
	}
	if delta.Summary == nil {
		return fmt.Errorf("computed delta requires summary")
	}
	switch delta.RowIdentity {
	case DeltaRowIdentityPosition:
		if len(seen) != 0 {
			return fmt.Errorf("positional delta forbids key evidence")
		}
	case DeltaRowIdentityBusinessKey:
		if uint64(len(evidence.DeltaAdded)) != delta.Summary.Added ||
			uint64(len(evidence.DeltaRemoved)) != delta.Summary.Removed || uint64(len(evidence.DeltaChanged)) != delta.Summary.Changed || delta.Summary.OrderChanged {
			return fmt.Errorf("business-key delta evidence contradicts summary")
		}
	default:
		return fmt.Errorf("computed delta has unsupported row identity")
	}
	return nil
}

func (r SourceOperationRecord) Validate() error {
	if err := r.Result.Validate(); err != nil {
		return err
	}
	for _, defect := range r.Evidence.Defects {
		if err := defect.Validate(); err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(r.Result.Defects, FirstEvidencePage(r.Evidence.Defects)) {
		return fmt.Errorf("source defects page contradicts complete evidence")
	}
	if err := validateDeltaEvidence(r.Evidence, r.Result.Delta); err != nil {
		return err
	}
	if r.Result.Outcome != "validation_rejected" && len(r.Evidence.Defects) != 0 {
		return fmt.Errorf("source outcome forbids defect evidence")
	}
	return nil
}

func (r SourceOperationRecord) ValidateForCommand(command SourceCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if err := r.Result.ValidateForCommand(command); err != nil {
		return err
	}
	return r.Evaluation.ValidateRecord(command, r)
}

func (r SourceOperationResult) ValidateForCommand(command SourceCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.SourceInvocationID != command.SourceInvocationID || r.Operation != command.Operation ||
		r.BundleHash != command.BundleHash || r.Declaration != command.Declaration || r.ExpectedHead != command.ExpectedHead {
		return fmt.Errorf("source result contradicts its canonical request")
	}
	return nil
}

func (s SourceOperationSummary) Validate() error {
	if s.DefectCount < 0 {
		return fmt.Errorf("source defect_count must not be negative")
	}
	result := SourceOperationResult{
		SourceInvocationID: s.SourceInvocationID, Operation: s.Operation, Outcome: s.Outcome, BundleHash: s.BundleHash,
		SchemaDigest: s.SchemaDigest, Declaration: s.Declaration, ExpectedHead: s.ExpectedHead, ObservedHead: s.ObservedHead,
		Candidate: s.Candidate, Head: s.Head, Delta: s.Delta, Defects: FirstEvidencePage([]ValidationDefect{}), CompletedAt: s.CompletedAt,
	}
	if s.Outcome == "validation_rejected" {
		if s.DefectCount == 0 {
			return fmt.Errorf("validation_rejected source summary requires defects")
		}
		// The summary intentionally carries only the total, not synthetic defects.
		result.Defects = FirstEvidencePage([]ValidationDefect{{Code: "summary", Message: "summary"}})
	} else {
		if s.DefectCount != 0 {
			return fmt.Errorf("source summary outcome forbids defects")
		}
		result.Defects = FirstEvidencePage([]ValidationDefect{})
	}
	return validateSourceOperationResult(result)
}

func (e FusedChildEvaluation) Validate() error {
	if err := validateCanonicalUUID(e.SourceInvocationID, "source_invocation_id"); err != nil {
		return err
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(e.BundleHash); err != nil {
		return err
	}
	if err := e.SchemaDigest.Validate(); err != nil {
		return err
	}
	if err := e.Declaration.Validate(); err != nil {
		return err
	}
	if err := e.ExpectedHead.Validate(); err != nil {
		return err
	}
	if err := e.ObservedHead.Validate(); err != nil {
		return err
	}
	if err := e.ValidateCandidateState(); err != nil {
		return err
	}
	if e.Candidate.Manifest != nil && (e.Candidate.Manifest.Declaration != e.Declaration || e.Candidate.Manifest.SchemaDigest != e.SchemaDigest) {
		return fmt.Errorf("fused child candidate contradicts declaration or schema")
	}
	if err := e.Delta.Validate(); err != nil {
		return err
	}
	if e.DefectCount < 0 {
		return fmt.Errorf("fused child defect_count must not be negative")
	}
	switch e.Outcome {
	case "ready":
		if e.DefectCount != 0 || !e.ExpectedHead.Equal(e.ObservedHead) || e.Delta.State == "not_computed" || e.Delta.Against == nil || !e.Delta.Against.Equal(e.ObservedHead) {
			return fmt.Errorf("ready fused child has contradictory head, delta, or defects")
		}
	case "head_conflict":
		if e.DefectCount != 0 || e.ExpectedHead.Equal(e.ObservedHead) || e.Delta.State != "not_computed" || e.Delta.Reason != "head_conflict" {
			return fmt.Errorf("head-conflict fused child has contradictory head, delta, or defects")
		}
	case "validation_rejected":
		if e.DefectCount == 0 || e.Delta.State != "not_computed" || e.Delta.Reason != "validation_rejected" {
			return fmt.Errorf("validation-rejected fused child has contradictory delta or defects")
		}
	default:
		return fmt.Errorf("fused child outcome is unsupported")
	}
	return nil
}

func (p Pin) Validate() error {
	if err := validateCanonicalUUID(p.RunID, "pin run_id"); err != nil {
		return err
	}
	if err := validateCanonicalRunState(p.RunState, "pin run_state"); err != nil {
		return err
	}
	if err := p.Declaration.Validate(); err != nil {
		return err
	}
	if err := p.SchemaDigest.Validate(); err != nil {
		return err
	}
	if err := p.VersionID.Validate(); err != nil {
		return err
	}
	switch p.Selection {
	case "explicit", "fused_import", "fork_inherited", "fork_override":
		return nil
	default:
		return fmt.Errorf("pin selection is unsupported")
	}
}

func (i RunCreationDataItem) Validate() error {
	switch i.Kind {
	case "pin":
		if i.Pin == nil || i.Import != nil {
			return fmt.Errorf("pin run-binding item requires pin only")
		}
		return i.Pin.Validate()
	case "import":
		if i.Import == nil || i.Pin != nil {
			return fmt.Errorf("import run-binding item requires import only")
		}
		return i.Import.Validate()
	default:
		return fmt.Errorf("run-binding item kind is unsupported")
	}
}

func (b DataBinding) Validate() error {
	switch b.State {
	case "none":
		if b.RunID != "" || b.PinCount != 0 || b.ImportCount != 0 || b.Evidence != nil {
			return fmt.Errorf("none data binding forbids binding facts")
		}
		return nil
	case "bound":
		if err := validateCanonicalUUID(b.RunID, "data binding run_id"); err != nil {
			return err
		}
		if b.PinCount < 1 || b.PinCount > MaxDataDeclarationsPerBundle || b.ImportCount < 0 || b.ImportCount > b.PinCount || b.Evidence == nil {
			return fmt.Errorf("bound data binding has invalid counts or evidence")
		}
		if err := b.Evidence.Validate(); err != nil {
			return err
		}
		for _, item := range b.Evidence.Items {
			if err := item.Validate(); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("data binding state must be none or bound")
	}
}

func (s RunCreationOperationSummary) Validate() error {
	if s.Kind != "run_creation" {
		return fmt.Errorf("run-creation summary kind must be run_creation")
	}
	if err := validateCanonicalUUID(s.RunID, "run_id"); err != nil {
		return err
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(s.BundleHash); err != nil {
		return err
	}
	if s.PinCount < 0 || s.PinCount > MaxDataDeclarationsPerBundle || s.ImportCount < 0 || s.ImportCount > MaxDataDeclarationsPerBundle {
		return fmt.Errorf("run-creation summary counts are outside supported bounds")
	}
	if err := s.Rejection.ValidateForOutcome(s.Outcome); err != nil {
		return err
	}
	if err := validateCompletedAt(s.CompletedAt); err != nil {
		return err
	}
	if s.Outcome == "created" {
		if err := validateCanonicalUUID(s.EventID, "event_id"); err != nil {
			return err
		}
		if err := validateCanonicalRunState(s.Status, "run-creation summary status"); err != nil {
			return err
		}
		return nil
	}
	if s.Outcome != "data_rejected" && s.Outcome != "head_conflict" {
		return fmt.Errorf("run-creation outcome is unsupported")
	}
	if s.EventID != "" || s.Status != "" || s.PinCount != 0 {
		return fmt.Errorf("rejected run-creation summary forbids committed run facts")
	}
	return nil
}

func (d FusedChildDefect) Validate() error {
	if err := validateCanonicalUUID(d.SourceInvocationID, "source_invocation_id"); err != nil {
		return err
	}
	return d.Defect.Validate()
}

func (r RunCreationOperationRecord) Validate() error {
	if err := r.Summary.Validate(); err != nil {
		return err
	}
	if err := r.Binding.Validate(); err != nil {
		return err
	}
	children := make(map[string]FusedChildEvaluation, len(r.Evidence.ChildEvaluations))
	declarations := make(map[string]struct{}, len(r.Evidence.ChildEvaluations))
	for index, child := range r.Evidence.ChildEvaluations {
		if err := child.Validate(); err != nil {
			return fmt.Errorf("fused child %s: %w", child.SourceInvocationID, err)
		}
		if _, duplicate := children[child.SourceInvocationID]; duplicate {
			return fmt.Errorf("run creation repeats fused child source_invocation_id %s", child.SourceInvocationID)
		}
		if _, duplicate := declarations[child.Declaration.Key()]; duplicate {
			return fmt.Errorf("run creation repeats fused child declaration %s", child.Declaration.Key())
		}
		if index > 0 && CompareDeclarationRef(r.Evidence.ChildEvaluations[index-1].Declaration, child.Declaration) >= 0 {
			return fmt.Errorf("fused child evaluations must be strictly declaration-sorted")
		}
		children[child.SourceInvocationID] = child
		declarations[child.Declaration.Key()] = struct{}{}
	}
	defectCounts := make(map[string]int, len(children))
	for _, defect := range r.Evidence.ChildDefects {
		if err := defect.Validate(); err != nil {
			return err
		}
		if _, found := children[defect.SourceInvocationID]; !found {
			return fmt.Errorf("child defect references unknown source invocation")
		}
		defectCounts[defect.SourceInvocationID]++
	}
	for id, child := range children {
		if defectCounts[id] != child.DefectCount {
			return fmt.Errorf("fused child %s defect_count contradicts evidence", id)
		}
	}
	for _, item := range r.Evidence.RunBinding {
		if err := item.Validate(); err != nil {
			return err
		}
	}

	if r.Summary.Outcome != "created" {
		if r.Binding.State != "none" || len(r.Evidence.RunBinding) != 0 || len(r.Evidence.ChildEvaluations) != r.Summary.ImportCount {
			return fmt.Errorf("rejected run creation has contradictory binding or child count")
		}
		return validateRejectedChildren(r)
	}
	if len(r.Evidence.ChildEvaluations) != 0 || len(r.Evidence.ChildDefects) != 0 {
		return fmt.Errorf("created run creation forbids pre-commit child evidence")
	}
	if r.Summary.PinCount == 0 {
		if r.Binding.State != "none" || r.Summary.ImportCount != 0 || len(r.Evidence.RunBinding) != 0 {
			return fmt.Errorf("unbound created run has contradictory counts or evidence")
		}
		return nil
	}
	if r.Binding.State != "bound" || r.Binding.RunID != r.Summary.RunID || r.Binding.PinCount != r.Summary.PinCount ||
		r.Binding.ImportCount != r.Summary.ImportCount || r.Binding.Evidence == nil ||
		!reflect.DeepEqual(*r.Binding.Evidence, FirstEvidencePage(r.Evidence.RunBinding)) {
		return fmt.Errorf("created run summary, binding, and evidence disagree")
	}
	return validateCreatedRunBinding(r)
}

func validateRejectedChildren(r RunCreationOperationRecord) error {
	validationRejected := false
	headConflict := false
	for _, child := range r.Evidence.ChildEvaluations {
		validationRejected = validationRejected || child.Outcome == "validation_rejected"
		headConflict = headConflict || child.Outcome == "head_conflict"
	}
	switch r.Summary.Rejection.Code {
	case RunCreationRejectionFusedValidation:
		if !validationRejected {
			return fmt.Errorf("fused validation rejection lacks rejected child evidence")
		}
	case RunCreationRejectionFusedHead:
		if validationRejected || !headConflict {
			return fmt.Errorf("fused head rejection contradicts child evidence")
		}
	default:
		if validationRejected || headConflict {
			return fmt.Errorf("explicit-pin rejection requires all fused children ready")
		}
	}
	return nil
}

func validateCreatedRunBinding(r RunCreationOperationRecord) error {
	pins := make(map[string]Pin, r.Summary.PinCount)
	imports := make(map[string]SourceOperationSummary, r.Summary.ImportCount)
	var previous *DeclarationRef
	for index := 0; index < len(r.Evidence.RunBinding); {
		pinItem := r.Evidence.RunBinding[index]
		if pinItem.Kind != "pin" || pinItem.Pin == nil {
			return fmt.Errorf("run binding must group each declaration under its pin")
		}
		pin := *pinItem.Pin
		if pin.RunID != r.Summary.RunID || pin.RunState != r.Summary.Status || (pin.Selection != "explicit" && pin.Selection != "fused_import") {
			return fmt.Errorf("run-binding pin contradicts parent summary")
		}
		if previous != nil && CompareDeclarationRef(*previous, pin.Declaration) >= 0 {
			return fmt.Errorf("run-binding pins must be strictly declaration-sorted")
		}
		copyRef := pin.Declaration
		previous = &copyRef
		if _, duplicate := pins[pin.Declaration.Key()]; duplicate {
			return fmt.Errorf("run binding repeats pin declaration %s", pin.Declaration.Key())
		}
		pins[pin.Declaration.Key()] = pin
		index++
		if index < len(r.Evidence.RunBinding) && r.Evidence.RunBinding[index].Kind == "import" {
			item := r.Evidence.RunBinding[index]
			if item.Import == nil || item.Import.Operation != "import" || item.Import.Outcome != "accepted" ||
				item.Import.BundleHash != r.Summary.BundleHash || item.Import.Declaration != pin.Declaration || pin.Selection != "fused_import" ||
				item.Import.Candidate.VersionID != pin.VersionID || item.Import.SchemaDigest != pin.SchemaDigest {
				return fmt.Errorf("bound import contradicts its fused-import pin")
			}
			if _, duplicate := imports[item.Import.SourceInvocationID]; duplicate {
				return fmt.Errorf("run binding repeats import source_invocation_id %s", item.Import.SourceInvocationID)
			}
			imports[item.Import.SourceInvocationID] = *item.Import
			index++
		} else if pin.Selection == "fused_import" {
			return fmt.Errorf("fused-import pin requires its source summary")
		}
	}
	if len(pins) != r.Summary.PinCount || len(imports) != r.Summary.ImportCount {
		return fmt.Errorf("run-binding item counts contradict summary")
	}
	return nil
}

func (r RunCreationOperationRecord) ValidateForCommand(command RunCreationCommand) error {
	_, _, canonical, err := command.RequestHash()
	if err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Summary.RunID != canonical.RunID || r.Summary.BundleHash != canonical.BundleHash || r.Summary.ImportCount != len(canonical.Data.Imports) {
		return fmt.Errorf("run-creation summary contradicts its canonical request")
	}
	if r.Summary.Outcome == "created" {
		if r.Summary.EventID != canonical.EventID || r.Summary.PinCount != len(canonical.Data.Imports)+len(canonical.Data.Pins) {
			return fmt.Errorf("created run summary contradicts requested event or data selection")
		}
		return validateCreatedRunRequest(r, canonical)
	}
	if len(r.Evidence.ChildEvaluations) != len(canonical.Data.Imports) {
		return fmt.Errorf("rejected run child evidence contradicts requested imports")
	}
	for index, item := range canonical.Data.Imports {
		child := r.Evidence.ChildEvaluations[index]
		if child.SourceInvocationID != item.SourceInvocationID || child.BundleHash != canonical.BundleHash || child.Declaration != item.Declaration || child.ExpectedHead != item.ExpectedHead {
			return fmt.Errorf("rejected run child evidence contradicts requested import")
		}
	}
	if target := r.Summary.Rejection.Declaration; target != nil {
		found := false
		for _, pin := range canonical.Data.Pins {
			found = found || (pin.Declaration == *target && pin.VersionID == r.Summary.Rejection.VersionID)
		}
		if !found {
			return fmt.Errorf("run rejection target contradicts requested explicit pins")
		}
	}
	return nil
}

func validateCreatedRunRequest(r RunCreationOperationRecord, command RunCreationCommand) error {
	requestedImports := make(map[string]FusedImport, len(command.Data.Imports))
	requestedPins := make(map[string]ExplicitPin, len(command.Data.Pins))
	for _, item := range command.Data.Imports {
		requestedImports[item.Declaration.Key()] = item
	}
	for _, item := range command.Data.Pins {
		requestedPins[item.Declaration.Key()] = item
	}
	for _, item := range r.Evidence.RunBinding {
		switch item.Kind {
		case "pin":
			pin := *item.Pin
			if imported, ok := requestedImports[pin.Declaration.Key()]; ok {
				if pin.Selection != "fused_import" {
					return fmt.Errorf("requested fused import became a non-import pin")
				}
				_ = imported
			} else if explicit, ok := requestedPins[pin.Declaration.Key()]; !ok || pin.Selection != "explicit" || pin.VersionID != explicit.VersionID {
				return fmt.Errorf("run-binding pin contradicts requested selection")
			}
		case "import":
			requested, ok := requestedImports[item.Import.Declaration.Key()]
			if !ok || item.Import.SourceInvocationID != requested.SourceInvocationID || item.Import.BundleHash != command.BundleHash || item.Import.ExpectedHead != requested.ExpectedHead {
				return fmt.Errorf("bound import contradicts requested fused import")
			}
		}
	}
	return nil
}

func (d PruneDefect) Validate() error {
	if (d.Code != "declaration_not_found" && d.Code != "version_not_found") || strings.TrimSpace(d.Message) == "" {
		return fmt.Errorf("prune defect is unsupported or incomplete")
	}
	return nil
}

func (r PruneOperationResult) Validate() error {
	if err := validateCanonicalUUID(r.PruneInvocationID, "prune_invocation_id"); err != nil {
		return err
	}
	if err := r.Declaration.Validate(); err != nil {
		return err
	}
	if err := r.VersionID.Validate(); err != nil {
		return err
	}
	if err := r.ExpectedHead.Validate(); err != nil {
		return err
	}
	if err := r.ObservedHead.Validate(); err != nil {
		return err
	}
	if err := validateCompletedAt(r.CompletedAt); err != nil {
		return err
	}
	if r.PinCount < 0 {
		return fmt.Errorf("prune pin_count must not be negative")
	}
	if r.Pins != nil {
		if err := r.Pins.Validate(); err != nil {
			return err
		}
		for _, pin := range r.Pins.Items {
			if err := pin.Validate(); err != nil {
				return err
			}
			if pin.Declaration != r.Declaration || pin.VersionID != r.VersionID {
				return fmt.Errorf("prune pin summary contradicts target")
			}
		}
	}
	if r.Defects != nil {
		if err := r.Defects.Validate(); err != nil {
			return err
		}
		for _, defect := range r.Defects.Items {
			if err := defect.Validate(); err != nil {
				return err
			}
		}
		if !reflect.DeepEqual(*r.Defects, FirstEvidencePage(r.Defects.Items)) {
			return fmt.Errorf("prune defect page is not complete canonical evidence")
		}
	}

	noPins := r.PinCount == 0 && r.Pins == nil
	noDefects := r.Defects == nil
	noPayload := r.PayloadBefore == "" && r.PayloadAfter == ""
	notCurrent := r.CurrentVersionID == ""
	switch r.Outcome {
	case "rejected":
		if !noPins || !noPayload || !notCurrent || r.Defects == nil || r.Defects.ItemCount != 1 {
			return fmt.Errorf("rejected prune has contradictory decision facts")
		}
	case "head_conflict":
		if !noPins || !noDefects || !noPayload || !notCurrent || r.ExpectedHead.Equal(r.ObservedHead) {
			return fmt.Errorf("head-conflict prune has contradictory decision facts")
		}
	case "refused_current":
		if !noPins || !noDefects || !noPayload || !r.ExpectedHead.Equal(r.ObservedHead) || r.ObservedHead != VersionHead(r.VersionID) || r.CurrentVersionID != r.VersionID {
			return fmt.Errorf("current-version prune refusal has contradictory decision facts")
		}
	case "refused_pinned":
		if r.PinCount < 1 || r.Pins == nil || !noDefects || !noPayload || !notCurrent || !r.ExpectedHead.Equal(r.ObservedHead) || r.ObservedHead == VersionHead(r.VersionID) {
			return fmt.Errorf("pinned prune refusal has contradictory decision facts")
		}
	case "already_pruned":
		if !noPins || !noDefects || !notCurrent || !r.ExpectedHead.Equal(r.ObservedHead) || r.ObservedHead == VersionHead(r.VersionID) || r.PayloadBefore != "pruned" || r.PayloadAfter != "pruned" {
			return fmt.Errorf("already-pruned result has contradictory decision facts")
		}
	case "pruned":
		if !noPins || !noDefects || !notCurrent || !r.ExpectedHead.Equal(r.ObservedHead) || r.ObservedHead == VersionHead(r.VersionID) || r.PayloadBefore != "materialized" || r.PayloadAfter != "pruned" {
			return fmt.Errorf("pruned result has contradictory decision facts")
		}
	default:
		return fmt.Errorf("prune outcome is unsupported")
	}
	return nil
}

func (r PruneOperationResult) ValidateWithPins(pins []Pin) error {
	if err := r.Validate(); err != nil {
		return err
	}
	copyPins := append([]Pin(nil), pins...)
	SortPins(copyPins)
	if !reflect.DeepEqual(copyPins, pins) {
		return fmt.Errorf("prune pin evidence is not canonically ordered")
	}
	for _, pin := range copyPins {
		if err := pin.Validate(); err != nil {
			return err
		}
		if pin.Declaration != r.Declaration || pin.VersionID != r.VersionID {
			return fmt.Errorf("prune pin evidence contradicts target")
		}
	}
	for index := 1; index < len(copyPins); index++ {
		if copyPins[index-1].RunID == copyPins[index].RunID {
			return fmt.Errorf("prune pin evidence repeats run_id %s", copyPins[index].RunID)
		}
	}
	if r.Outcome != "refused_pinned" {
		if len(copyPins) != 0 {
			return fmt.Errorf("prune outcome forbids complete pin evidence")
		}
		return nil
	}
	if r.PinCount != len(copyPins) || r.Pins == nil || !reflect.DeepEqual(*r.Pins, FirstEvidencePage(copyPins)) {
		return fmt.Errorf("prune pin evidence is incomplete")
	}
	return nil
}

func (r PruneOperationResult) ValidateForCommand(command PruneCommand, pins []Pin) error {
	if _, _, err := command.RequestHash(); err != nil {
		return err
	}
	if err := r.ValidateWithPins(pins); err != nil {
		return err
	}
	return r.ValidateRequest(command)
}

func (r PruneOperationResult) ValidateRequest(command PruneCommand) error {
	if _, _, err := command.RequestHash(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.PruneInvocationID != command.PruneInvocationID || r.Declaration != command.Declaration || r.VersionID != command.VersionID || r.ExpectedHead != command.ExpectedHead {
		return fmt.Errorf("prune result contradicts its canonical request")
	}
	return nil
}
