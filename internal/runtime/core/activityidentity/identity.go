package activityidentity

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/google/uuid"
)

type Fact struct {
	RunID           string
	SourceEventID   string
	ParentEventID   string
	EntityID        string
	FlowID          string
	NodeID          string
	HandlerEventKey string
	ActivityID      string
	Tool            string
	Attempt         int
	RevisionID      string
}

func RequestEventID(fact Fact) string {
	parts := []string{
		fact.RunID, fact.SourceEventID, fact.ParentEventID, fact.EntityID,
		fact.FlowID, fact.NodeID, fact.HandlerEventKey, fact.ActivityID,
		fmt.Sprintf("%d", fact.Attempt), fact.RevisionID,
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-request:"+strings.Join(parts, "\x00"))).String()
}

func ResultEventID(fact Fact, eventType string) string {
	parts := []string{
		fact.RunID, fact.SourceEventID, fact.ParentEventID, fact.EntityID,
		fact.FlowID, fact.NodeID, fact.HandlerEventKey, fact.ActivityID,
		fact.Tool, eventidentity.Normalize(eventType), fact.RevisionID,
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-result:"+strings.Join(parts, "\x00"))).String()
}

func ForkLineageEventID(forkRunID, sourceEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:fork-event-lineage:"+strings.TrimSpace(forkRunID)+"\x00"+strings.TrimSpace(sourceEventID))).String()
}
