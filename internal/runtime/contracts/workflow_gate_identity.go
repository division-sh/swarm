package contracts

import (
	"fmt"
	"sort"
	"strings"
)

// ValidateCanonicalWorkflowGatePlanIdentity rejects programmatic plans whose
// map identities differ from the normalized identities produced by YAML
// decoding. Immutable card hashes and API lookups must see the same keys.
func ValidateCanonicalWorkflowGatePlanIdentity(plan WorkflowGatePlan) error {
	return ValidateCanonicalWorkflowGateSnapshotIdentity(plan.Decision, mapKeys(plan.Context), plan.Outcomes)
}

// ValidateCanonicalWorkflowGateSnapshotIdentity applies the gate map identity
// invariant to materialized snapshots before hashing or persistence.
func ValidateCanonicalWorkflowGateSnapshotIdentity(decision string, contextKeys []string, outcomes map[string]WorkflowGateOutcomePlan) error {
	if err := validateCanonicalGateScalar("decision id", decision); err != nil {
		return err
	}
	if err := validateCanonicalGateKeys("context field", contextKeys); err != nil {
		return err
	}
	verdicts := mapKeys(outcomes)
	if err := validateCanonicalGateKeys("verdict", verdicts); err != nil {
		return err
	}
	sort.Strings(verdicts)
	for _, verdict := range verdicts {
		outcome := outcomes[verdict]
		if outcome.Verdict != "" {
			if err := validateCanonicalGateScalar(fmt.Sprintf("outcome %s verdict", verdict), outcome.Verdict); err != nil {
				return err
			}
			if outcome.Verdict != verdict {
				return fmt.Errorf("outcome %s carries mismatched verdict identity %q", verdict, outcome.Verdict)
			}
		}
		if err := validateCanonicalGateKeys(fmt.Sprintf("outcome %s input field", verdict), mapKeys(outcome.Input)); err != nil {
			return err
		}
		if err := validateCanonicalGateKeys(fmt.Sprintf("outcome %s emit field", verdict), mapKeys(outcome.Emit.Fields)); err != nil {
			return err
		}
	}
	return nil
}

func validateCanonicalGateScalar(label, raw string) error {
	canonical := strings.TrimSpace(raw)
	if canonical == "" {
		return fmt.Errorf("stage gate %s is empty", label)
	}
	if raw != canonical {
		return fmt.Errorf("stage gate %s %q is not canonical; use %q", label, raw, canonical)
	}
	return nil
}

func validateCanonicalGateKeys(label string, keys []string) error {
	sort.Strings(keys)
	seen := map[string]string{}
	for _, raw := range keys {
		canonical := strings.TrimSpace(raw)
		if canonical == "" {
			return fmt.Errorf("stage gate %s is empty", label)
		}
		if previous, ok := seen[canonical]; ok {
			return fmt.Errorf("stage gate %s contains duplicate normalized key %q (from %q and %q)", label, canonical, previous, raw)
		}
		seen[canonical] = raw
	}
	for _, raw := range keys {
		canonical := strings.TrimSpace(raw)
		if raw != canonical {
			return fmt.Errorf("stage gate %s key %q is not canonical; use %q", label, raw, canonical)
		}
	}
	return nil
}

func mapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
