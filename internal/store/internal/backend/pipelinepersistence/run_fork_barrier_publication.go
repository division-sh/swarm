package pipelinepersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func runForkFanOutBarrierTransferredOccurrence(source fanoutbarrier.Barrier, schedule *genericschedule.Activation, publication genericschedule.JoinContinuation, child fanoutbarrier.Registration) (*fanoutbarrier.TransferredOccurrence, error) {
	if source.Status != fanoutbarrier.StatusClosedPending || source.Summary == nil || !publication.Present() {
		return nil, fmt.Errorf("published barrier materialization requires exact unfinished work")
	}
	original := publication.SourceEvent()
	if publication.SourceRunID() != source.Registration.IntentKey.RunID {
		return nil, fmt.Errorf("published barrier materialization contradicts its source owner")
	}
	var due = publication.ChildCommand().Due.Absolute
	if source.TransferredOccurrence != nil {
		if schedule != nil {
			return nil, fmt.Errorf("transferred barrier cannot also own a schedule")
		}
		occurrence, err := genericschedule.NewTransferredFanOutBarrierOccurrence(source)
		if err != nil {
			return nil, err
		}
		if retained, pending := publication.PendingSource(); pending {
			want, err := occurrence.EvidenceDigest()
			if err != nil {
				return nil, err
			}
			got, err := retained.EvidenceDigest()
			if err != nil || got != want {
				return nil, fmt.Errorf("pending barrier contradicts its exact retained source obligation")
			}
		} else if err := occurrence.ValidateEvent(original); err != nil {
			return nil, err
		}
		if !due.Equal(occurrence.Command.Due.Absolute) {
			return nil, fmt.Errorf("transferred barrier changes original due")
		}
	} else {
		if schedule == nil {
			return nil, fmt.Errorf("published barrier requires exact source occurrence")
		}
		if err := genericschedule.ValidateFanOutBarrierScheduleRelation(source, *schedule); err != nil {
			return nil, err
		}
		if _, err := schedule.ValidatePublishedOccurrence(original); err != nil {
			return nil, err
		}
		if !due.Equal(schedule.Command.Due.Absolute) {
			return nil, fmt.Errorf("published barrier changes original due")
		}
	}
	expected, err := genericschedule.FanOutBarrierAdmission(child, *source.Summary, due)
	if err != nil {
		return nil, err
	}
	want, err := expected.ImmutableHash()
	if err != nil {
		return nil, err
	}
	got, err := publication.ChildCommand().ImmutableHash()
	if err != nil || want != got {
		return nil, fmt.Errorf("published barrier contradicts exact child correspondence")
	}
	record, err := publication.RetainedPublication(runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		return nil, err
	}
	return &fanoutbarrier.TransferredOccurrence{Publication: record, DueAt: due}, nil
}
