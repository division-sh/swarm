package startupownership

import "context"

type generationGrantContextKey struct{}

// A detached descriptor carries the existing grant into its own control turn.
// It is never admission by itself; native readers and writers reprove it.
func WithGenerationGrantEvidence(ctx context.Context, evidence GrantEvidence) context.Context {
	return context.WithValue(ctx, generationGrantContextKey{}, evidence.clone())
}

func GenerationGrantEvidenceFromContext(ctx context.Context) (GrantEvidence, bool) {
	if ctx == nil {
		return GrantEvidence{}, false
	}
	evidence, present := ctx.Value(generationGrantContextKey{}).(GrantEvidence)
	if !present || evidence.Validate() != nil || evidence.State != GrantAdmitted {
		return GrantEvidence{}, false
	}
	return evidence.clone(), true
}
