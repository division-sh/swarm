package main

import (
  "context"
  "fmt"
  "time"
  "github.com/google/uuid"
  "empireai/internal/events"
  p "empireai/internal/runtime/pipeline"
  empire "empireai/internal/runtime/pipeline/empire"
)

func main() {
  p.SetDefaultWorkflowModuleFactory(func() p.WorkflowModule { return empire.NewModule() })
  bus := p.NewEventBus(p.InMemoryEventStore{})
  pc := p.NewFactoryPipelineCoordinator(bus, nil)
  bus.SetInterceptors(pc)
  scanID := uuid.NewString()
  err := bus.Publish(context.Background(), events.Event{ID: uuid.NewString(), Type: events.EventType("scan.requested"), SourceAgent: "empire-coordinator", Payload: []byte(fmt.Sprintf(`{"scan_id":%q,"mode":"local_services","geography":"Argentina","campaign_id":%q}`, scanID, uuid.NewString())), CreatedAt: time.Now().UTC()})
  fmt.Println("scan.requested", err)
  doneCh := bus.Subscribe("watch", events.EventType("scan.completed"))
  types := []events.EventType{"scanner.google_maps.scan_complete","scanner.instagram.scan_complete","scanner.reviews.scan_complete","scanner.directories.scan_complete","scanner.yelp.scan_complete"}
  for _, t := range types {
    err := bus.Publish(context.Background(), events.Event{ID: uuid.NewString(), Type: t, SourceAgent: "scanner-agent", Payload: []byte(fmt.Sprintf(`{"scan_id":%q}`, scanID)), CreatedAt: time.Now().UTC()})
    fmt.Println("publish", t, err)
  }
  select {
  case evt := <-doneCh:
    fmt.Println("DONE", evt.Type, string(evt.Payload))
  case <-time.After(2*time.Second):
    fmt.Println("NO DONE")
  }
}
