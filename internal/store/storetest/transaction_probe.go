package storetest

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type TransactionProbeOptions = transactiontest.Options
type TransactionCollector = transactiontest.Collector
type TransactionSnapshot = transactiontest.Snapshot
type TransactionCounts = transactiontest.Counts
type TransactionOperation = transactiontest.Operation
type TransactionDelayScope = transactiontest.DelayScope
type TransactionRevisionCounts = transactiontest.RevisionCounts
type TransactionActiveClass = transactiontest.ActiveClass
type TransactionActivePhase = transactiontest.ActivePhase

const (
	DelayAllCommits                         = transactiontest.DelayAllCommits
	DelayAllWrites                          = transactiontest.DelayAllWrites
	DelayServingWrites                      = transactiontest.DelayServingWrites
	TransactionOther                        = transactiontest.Other
	TransactionFanOutClaim                  = transactiontest.FanOutClaim
	TransactionFanOutChunk                  = transactiontest.FanOutChunk
	TransactionFanOutProducer               = transactiontest.FanOutProducer
	TransactionFanOutLoad                   = transactiontest.FanOutLoad
	TransactionFanOutObservation            = transactiontest.FanOutObservation
	TransactionDeliveryContinuationScan     = transactiontest.DeliveryContinuationScan
	TransactionDeliveryContinuationObserve  = transactiontest.DeliveryContinuationObserve
	TransactionDeliveryClaim                = transactiontest.DeliveryClaim
	TransactionDeliveryRenew                = transactiontest.DeliveryRenew
	TransactionDeliverySettle               = transactiontest.DeliverySettle
	TransactionWorkflowMutation             = transactiontest.WorkflowMutation
	TransactionFanOutRetry                  = transactiontest.FanOutRetry
	TransactionFanOutBlock                  = transactiontest.FanOutBlock
	TransactionFanOutRelease                = transactiontest.FanOutRelease
	TransactionPipelineDecisionProcessed    = transactiontest.PipelineDecisionProcessed
	TransactionPipelineSettlement           = transactiontest.PipelineSettlement
	TransactionPipelinePublicationAdmission = transactiontest.PipelinePublicationAdmission
	TransactionPipelineEligibility          = transactiontest.PipelineEligibility
	TransactionPipelineLoad                 = transactiontest.PipelineLoad
	TransactionRunExecutionInspection       = transactiontest.RunExecutionInspection
	TransactionSourceSetLoad                = transactiontest.SourceSetLoad
	TransactionRunCompletionCandidate       = transactiontest.RunCompletionCandidate
)

// CollectTransactions observes actual transaction-owner settlement. Write
// counts mean acknowledged commits of write-mode transactions, not API calls
// or SQL statement counts. Implicit autocommit SQL outside these owners is not
// included. Revision contains only finalizer SQL call counts and elapsed phases;
// it is not a workload SQL count or pure database lock-wait measurement.
// Installation is local to selected and restored by test cleanup;
// transactions already in flight retain the collector captured at BeginTx.
func CollectTransactions(t testing.TB, selected any, options TransactionProbeOptions) *TransactionCollector {
	t.Helper()
	collector, restore, err := private.InstallTransactionProbeForTest(selected, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restore)
	return collector
}

func ReplaceTransactions(t testing.TB, previous *TransactionCollector, options TransactionProbeOptions) *TransactionCollector {
	t.Helper()
	collector, err := previous.Replace(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Stop)
	return collector
}

func ValidateReporterIssuance(receipt TransactionSnapshot, submitted time.Time, chunks uint64, delay time.Duration) error {
	return transactiontest.ValidateReporterIssuance(receipt, submitted, chunks, delay)
}

func ValidateReporterContinuation(issuance, detached, successor TransactionSnapshot) error {
	return transactiontest.ValidateReporterContinuation(issuance, detached, successor)
}

func ValidateReporterJoined(issuance, detached, successor TransactionSnapshot) error {
	return transactiontest.ValidateReporterJoined(issuance, detached, successor)
}

func ValidateReporterStress(issuance, final TransactionSnapshot, delay time.Duration) error {
	return transactiontest.ValidateReporterStress(issuance, final, delay)
}

func ValidateReporterStressJoined(issuance, final TransactionSnapshot, delay time.Duration) error {
	return transactiontest.ValidateReporterStressJoined(issuance, final, delay)
}

func ValidateReporterDrain(issuance, detached, successor TransactionSnapshot, settledBeforeHandoff bool) error {
	return transactiontest.ValidateReporterDrain(issuance, detached, successor, settledBeforeHandoff)
}
