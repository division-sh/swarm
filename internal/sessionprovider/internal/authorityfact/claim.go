package authorityfact

import (
	"bytes"
	"context"
	"fmt"
	"time"
)

type ownedClaim struct {
	parentID       string
	parentRevision int64
	body           []byte
	receivedAt     time.Time
	ctx            context.Context
	fingerprint    string
	current        func() bool
}

type Claim struct{ value *ownedClaim }

// SealOwnedClaim is called only after native admission and compiled text mapping.
func SealOwnedClaim(parentID string, parentRevision int64, fingerprint string, body []byte, receivedAt time.Time, ctx context.Context, current func() bool) Claim {
	return Claim{value: &ownedClaim{parentID: parentID, parentRevision: parentRevision,
		fingerprint: fingerprint, body: bytes.Clone(body), receivedAt: receivedAt, ctx: ctx, current: current}}
}

func (c Claim) Validate(ctx context.Context) error {
	if c.value == nil || ctx == nil || ctx.Err() != nil || c.value.ctx == nil || c.value.ctx.Err() != nil ||
		c.value.parentID == "" || c.value.parentRevision < 1 || c.value.fingerprint == "" || len(c.value.body) == 0 || c.value.receivedAt.IsZero() || c.value.current == nil || !c.value.current() {
		return fmt.Errorf("native claim requires its original current admitted input")
	}
	return nil
}

func (c Claim) Parent() (string, int64) {
	if c.value == nil {
		return "", 0
	}
	return c.value.parentID, c.value.parentRevision
}

func (c Claim) Body() []byte {
	if c.value == nil {
		return nil
	}
	return bytes.Clone(c.value.body)
}

func (c Claim) ReceivedAt() time.Time {
	if c.value == nil {
		return time.Time{}
	}
	return c.value.receivedAt
}

func (c Claim) Fingerprint() string {
	if c.value == nil {
		return ""
	}
	return c.value.fingerprint
}
