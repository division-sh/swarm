package events

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestConnectCandidateSortPreservesComparisonAndConflicts(t *testing.T) {
	var candidates []ConnectCandidateEvidence
	for i := 0; i < 128; i++ {
		receiver := AdmitConnectReceiverIdentity(sha256.Sum256([]byte(fmt.Sprintf("receiver-%d", i%7))))
		recipient := MustNodeDeliveryRecipient(identitytest.RootNode(t, fmt.Sprintf("node-%d", i%11)))
		var plan agentidentity.Plan
		if i%3 == 0 {
			name, err := agentidentity.DeclaredName("agent", fmt.Sprintf("owner-%d", i%13))
			if err != nil {
				t.Fatal(err)
			}
			plan, err = agentidentity.NewPlan(name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			recipient = MustAgentDeliveryRecipient("agent")
		}
		candidate, err := NewConnectCandidateEvidence(receiver, recipient, fmt.Sprintf("child/%d", i), plan, ConnectCandidateOutcome(i%3+1))
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate, candidate)
	}
	rng := rand.New(rand.NewSource(2394))
	for permutation := 0; permutation < 20; permutation++ {
		rng.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
		before := append([]ConnectCandidateEvidence(nil), candidates...)
		want := append([]ConnectCandidateEvidence(nil), candidates...)
		key := func(c ConnectCandidateEvidence) string {
			return c.receiver.String() + "\x00" + c.recipient.Code() + "\x00" + c.recipient.ID() + "\x00" + c.path + "\x00" + c.agent.Description() + "\x00" + c.outcome.Code()
		}
		sort.Slice(want, func(i, j int) bool { return key(want[i]) < key(want[j]) })
		compacted := want[:0]
		for _, candidate := range want {
			if len(compacted) == 0 || !sameCandidateIdentity(compacted[len(compacted)-1], candidate) {
				compacted = append(compacted, candidate)
			}
		}
		got, err := normalizeCandidateEvidence(candidates)
		if err != nil || !reflect.DeepEqual(got, compacted) || !reflect.DeepEqual(candidates, before) {
			t.Fatalf("permutation %d changed order, identity, duplicate treatment or source: %v", permutation, err)
		}
		contradiction := candidates[0]
		contradiction.outcome = ConnectCandidateOutcome(int(contradiction.outcome)%3 + 1)
		if _, err := normalizeCandidateEvidence(append(before, contradiction)); err == nil {
			t.Fatalf("permutation %d admitted conflicting exact candidate", permutation)
		}
	}
	if got, err := normalizeCandidateEvidence(nil); got != nil || err != nil {
		t.Fatalf("empty evidence changed: %v %v", got, err)
	}
}
