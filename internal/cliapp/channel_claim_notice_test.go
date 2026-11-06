package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type claimNoticeClient struct {
	rows []channelOnboardingResult
	read int
}

func (c *claimNoticeClient) call(_ context.Context, method string, _ map[string]any, target any) error {
	result, ok := target.(*channelOnboardingResult)
	if !ok || method != "channel.onboarding_get" || c.read >= len(c.rows) {
		return fmt.Errorf("unexpected claim read %s at %d", method, c.read)
	}
	*result = c.rows[c.read]
	c.read++
	return nil
}

func TestChannelClaimNoticeCoalescesWithoutChangingClaimWait(t *testing.T) {
	base := channelOnboardingResult{Operation: channelonboarding.Operation{OperationID: "onboarding"},
		IdentityOperation: &operatorchannel.Operation{OperationID: "identity", State: operatorchannel.StateAwaitingClaim, ExpiresAt: time.Now().Add(time.Minute)}}
	row := func(token string) channelOnboardingResult {
		current := base
		current.Connection = &channelonboarding.SessionConnectionReadback{ClaimCodeNotice: &channelonboarding.ClaimCodeNotice{IdentityOperationID: "identity", Observation: token}}
		return current
	}
	begun := row("before this invocation")
	client := &claimNoticeClient{rows: []channelOnboardingResult{row("before this invocation"), row("new snapshot"), row("new snapshot"), row("another snapshot")}}
	closed := row("after claim")
	identity := *base.IdentityOperation
	identity.State = operatorchannel.StateAwaitingConfirmation
	closed.IdentityOperation = &identity
	client.rows = append(client.rows, closed)
	opts := defaultRootCommandOptions()
	opts.channelConnectPoll = time.Millisecond
	var diagnostic bytes.Buffer
	result, err := waitForChannelOnboardingClaim(t.Context(), client, begun, opts, &diagnostic)
	if err != nil || result.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation || client.read != 5 || diagnostic.String() != "error: bad code\nerror: bad code\n" {
		t.Fatal("notice duplicated, consumed or failed the claim wait", diagnostic.String(), client.read, err)
	}
}

func TestChannelClaimNoticeRefusesForeignOrExpiredObservation(t *testing.T) {
	base := channelOnboardingResult{Operation: channelonboarding.Operation{OperationID: "onboarding"},
		IdentityOperation: &operatorchannel.Operation{OperationID: "identity", State: operatorchannel.StateAwaitingClaim, ExpiresAt: time.Now().Add(time.Minute)}}
	for _, test := range []string{"foreign identity", "foreign onboarding", "expired", "empty snapshot"} {
		t.Run(test, func(t *testing.T) {
			current := base
			notice := &channelonboarding.ClaimCodeNotice{IdentityOperationID: "identity", Observation: "new"}
			current.Connection = &channelonboarding.SessionConnectionReadback{ClaimCodeNotice: notice}
			switch test {
			case "foreign identity":
				notice.IdentityOperationID = "another identity"
			case "foreign onboarding":
				current.Operation.OperationID = "another onboarding"
			case "expired":
				identity := *base.IdentityOperation
				identity.ExpiresAt = time.Now().Add(-time.Second)
				current.IdentityOperation = &identity
			case "empty snapshot":
				notice.Observation = ""
			}
			closed := base
			closed.IdentityOperation = nil
			client := &claimNoticeClient{rows: []channelOnboardingResult{current, closed}}
			opts := defaultRootCommandOptions()
			opts.channelConnectPoll = time.Millisecond
			var diagnostic bytes.Buffer
			if _, err := waitForChannelOnboardingClaim(t.Context(), client, base, opts, &diagnostic); err != nil || strings.Contains(diagnostic.String(), "bad code") {
				t.Fatal("stale notice printed or failed the operation", diagnostic.String(), err)
			}
		})
	}
}
