package delivery

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (a *Adapter) profileForClaimTx(ctx context.Context, tx *sql.Tx, event events.Event, record deliveryRecord) ([]byte, deliverylifecycle.AgentClaimProfile, error) {
	var absent deliverylifecycle.AgentClaimProfile
	if !record.Route.Recipient.IsAgent() || record.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
		return nil, absent, nil
	}
	// The isolated mechanics fixture has no actor-admission owner and cannot
	// authorize a provider launch. Native selected-store assemblies require it.
	if a.receiverExecution == nil {
		return nil, absent, nil
	}
	if record.ClaimVersion > 0 {
		_, err := readFirstAgentClaimProfileTx(ctx, tx, record.Snapshot)
		return nil, absent, err
	}
	profile, err := a.receiverExecution.ReadReceiverClaimProfileTx(ctx, tx, event, record.Route, record.Authority)
	if err != nil {
		return nil, absent, err
	}
	if !profile.MatchesFirstAdmission(event, record.Route, record.Authority) {
		return nil, absent, fmt.Errorf("first agent profile differs from admitted claim identity")
	}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(profile)
	return raw, profile, err
}

// This is historical claim evidence, not live-generation authorization.
func ReadFirstAgentClaimProfileTx(ctx context.Context, tx *sql.Tx, dialect Dialect, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	var absent deliverylifecycle.AgentClaimProfile
	if tx == nil || expected.Authority.Validate() != nil || !expected.Route.Recipient.IsAgent() ||
		expected.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime || expected.ClaimVersion < 1 {
		return absent, fmt.Errorf("first agent profile requires exact ordinary claimed delivery evidence")
	}
	actual, err := ReadExactClaimedAgentSnapshotTx(ctx, tx, dialect, expected)
	if err != nil {
		return absent, err
	}
	return readFirstAgentClaimProfileTx(ctx, tx, actual)
}

// ReadExactClaimedAgentSnapshotTx admits historical evidence, not a live claim.
// The selected preparation reader composes it under the same read transaction.
func ReadExactClaimedAgentSnapshotTx(ctx context.Context, tx *sql.Tx, dialect Dialect, expected deliverylifecycle.Snapshot) (deliverylifecycle.Snapshot, error) {
	var absent deliverylifecycle.Snapshot
	if tx == nil || expected.Authority.Validate() != nil || !expected.Route.Recipient.IsAgent() || expected.ClaimVersion < 1 {
		return absent, fmt.Errorf("agent evidence requires exact claimed delivery identity")
	}
	adapter, err := NewAdapter(dialect)
	if err != nil {
		return absent, err
	}
	actual, err := adapter.loadByID(ctx, tx, expected.DeliveryID, false)
	if err != nil {
		return absent, err
	}
	if actual.EventID != expected.EventID || actual.RunID != expected.RunID ||
		!events.SameDeliveryRouteIdentity(actual.Route, expected.Route) || !actual.Authority.Equal(expected.Authority) || actual.ClaimVersion < 1 {
		return absent, fmt.Errorf("first agent profile differs from exact delivery/source authority")
	}
	return actual.Snapshot, nil
}

func readFirstAgentClaimProfileTx(ctx context.Context, tx *sql.Tx, snapshot deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	var profile deliverylifecycle.AgentClaimProfile
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT agent_claim_profile FROM event_delivery_attempts
		WHERE delivery_id=$1 AND claim_version=1`, snapshot.DeliveryID).Scan(&raw); err != nil {
		return profile, fmt.Errorf("read immutable first-claim agent profile: %w", err)
	}
	if len(raw) == 0 {
		return profile, fmt.Errorf("immutable first-claim agent profile is missing")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("decode immutable first-claim agent profile: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("immutable first-claim agent profile has trailing data")
	}
	if !profile.MatchesDelivery(snapshot) {
		return deliverylifecycle.AgentClaimProfile{}, fmt.Errorf("immutable first-claim agent profile is corrupt or foreign")
	}
	return profile, nil
}
