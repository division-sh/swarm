package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

func family145ResetCountBridge(t *testing.T, file, function, owner, identity string) {
	t.Helper()
	actual := family145ConversationForkOwnerSource(t, file, function)
	if err := family145ResetCountBridgeProofError(actual, function, owner, identity); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"validateChannelObservationOwner(selected)", "readServedDeliveryObservation(ctx, selected,", owner, "ctx, tx, " + identity, "return count, nil"} {
		mutant := strings.Replace(actual, cut, "foreignResetCountCut", 1)
		if mutant == actual || family145ResetCountBridgeProofError(mutant, function, owner, identity) == nil {
			t.Fatal("reset count bridge lost owner/identity/context/error semantics:", function, cut)
		}
	}
}

func family145ResetCountBridgeProofError(actual, function, owner, identity string) error {
	expected := fmt.Sprintf(`func %s(ctx context.Context, selected any, %s string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = %s(ctx, tx, %s)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}`, function, identity, owner, identity)
	got, err := requestkey.Normalize(actual)
	if err != nil {
		return err
	}
	want, err := requestkey.Normalize(expected)
	if err != nil || got != want {
		return fmt.Errorf("reset count bridge changed complete native owner/context/identity/error correspondence: %s: %v", function, err)
	}
	return nil
}
