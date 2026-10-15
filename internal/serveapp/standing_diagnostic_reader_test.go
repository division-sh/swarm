package serveapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func standingStoreDiagnostics(backend, location string) string {
	owner, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
	if err != nil {
		return err.Error()
	}
	defer owner.Close()
	return renderStandingStoreDiagnostics(context.Background(), owner)
}

// Each display section has its own completed inspection cut. A failed cut never
// contributes provisional rows; later failures only omit diagnostic text.
func renderStandingStoreDiagnostics(ctx context.Context, owner storetest.ReleaseProcessReadOnlyInspection) string {
	var names []storetest.StandingEventNameDiagnostic
	err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		names, err = storetest.ReadStandingEventNameDiagnostics(scoped, owner)
		return err
	})
	if err != nil {
		return err.Error()
	}
	parts := []string{}
	for _, row := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", row.Name, row.Count))
	}
	var logs []string
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		logs, err = storetest.ReadStandingRuntimeLogDiagnostics(scoped, owner)
		return err
	}); err == nil {
		parts = append(parts, logs...)
	}
	var deliveries []storetest.StandingDeliveryDiagnostic
	if err := owner.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		deliveries, err = storetest.ReadStandingDeliveryDiagnostics(scoped, owner)
		return err
	}); err == nil {
		for _, row := range deliveries {
			parts = append(parts, fmt.Sprintf("delivery:%s:%s:%s", row.EventID, row.Subscriber, row.Status))
		}
	}
	return strings.Join(parts, ",")
}
