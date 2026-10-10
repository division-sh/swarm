package authorityfact

import (
	"context"
	"fmt"
	"time"
)

// NonClaim cannot be converted into Claim or reconstructed from exported fields.
// Its only outcome is retention of non-executable setup-input evidence.
type NonClaim struct {
	original Claim
	check    func(context.Context) error
}

func SealOwnedNonClaim(parentID string, revision int64, fingerprint string, original []byte, receivedAt time.Time, ctx context.Context, current func() bool, check func(context.Context) error) NonClaim {
	return NonClaim{original: SealOwnedClaim(parentID, revision, fingerprint, original, receivedAt, ctx, current), check: check}
}

func (d NonClaim) Empty() bool { return d.original.value == nil }

func (d NonClaim) Validate(ctx context.Context) error {
	if err := d.original.Validate(ctx); err != nil {
		return fmt.Errorf("native non-claim disposition requires its original admitted input: %w", err)
	}
	if d.check == nil {
		return fmt.Errorf("native non-claim disposition has no selected responsibility owner")
	}
	return d.check(ctx)
}

func (d NonClaim) OriginalCapture() []byte { return d.original.Body() }
func (d NonClaim) Fingerprint() string     { return d.original.Fingerprint() }
func (d NonClaim) Parent() (string, int64) { return d.original.Parent() }
