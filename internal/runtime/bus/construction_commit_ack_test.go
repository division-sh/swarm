package bus

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestConstructionCommitAcknowledgmentPromotesExactTreeWithoutMutatingTransactionEvidence(t *testing.T) {
	transaction := CommittedPublication{
		AppendOutcome: EventAppendInserted,
		Activations: []CommittedFlowInstanceActivation{{
			Created: true, ReadinessAttemptOrdinal: 1,
			Children: []CommittedFlowInstanceActivation{{
				Created: true, ReadinessAttemptOrdinal: 1,
				Children: []CommittedFlowInstanceActivation{{Created: true, ReadinessAttemptOrdinal: 1}},
			}},
		}},
	}
	acknowledged := transaction.WithCommitAcknowledgment()
	engine := (CommittedEnginePublication{committed: transaction}).WithCommitAcknowledgment()
	for _, publication := range []CommittedPublication{acknowledged, engine.committed} {
		if !publication.Acknowledged || publication.AppendOutcome != transaction.AppendOutcome {
			t.Fatal("outer acknowledgment lost publication evidence")
		}
		var check func(pipeline.CommittedFlowInstanceActivation)
		check = func(activation pipeline.CommittedFlowInstanceActivation) {
			if !activation.Acknowledged || !activation.Created || activation.ReadinessAttemptOrdinal != 1 {
				t.Fatalf("descendant lost acknowledged construction evidence: %+v", activation)
			}
			for _, child := range activation.Children {
				check(child)
			}
		}
		check(publication.Activations[0])
		if !reflect.DeepEqual(publication, publication.WithCommitAcknowledgment()) {
			t.Fatal("repeated acknowledgment changed committed evidence")
		}
	}
	if transaction.Acknowledged || transaction.Activations[0].Acknowledged ||
		transaction.Activations[0].Children[0].Acknowledged ||
		transaction.Activations[0].Children[0].Children[0].Acknowledged {
		t.Fatal("outer acknowledgment mutated transaction-local evidence")
	}
}
