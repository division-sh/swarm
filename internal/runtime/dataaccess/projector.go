package dataaccess

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// Build projects the exact immutable bytes authorized for one concrete actor
// from compiled grants and the selected run's persisted version pins.
func Build(ctx context.Context, source semanticview.Source, store durabledata.ResourceAccessStore, runID string, actor models.AgentConfig) (durabledata.AccessList, error) {
	runID = strings.TrimSpace(runID)
	parsed, err := uuid.Parse(runID)
	if err != nil || parsed == uuid.Nil || parsed.String() != runID {
		return durabledata.AccessList{}, fmt.Errorf("data access run_id must be one canonical non-zero UUID")
	}
	identity, err := actor.ConcreteIdentity()
	if err != nil {
		return durabledata.AccessList{}, fmt.Errorf("data access actor identity: %w", err)
	}

	list := durabledata.AccessList{SchemaVersion: durabledata.AccessSchemaVersion, RunID: runID, AgentIdentity: identity}
	for _, item := range flowdata.AllowedStaticData(source, actor) {
		static := durabledata.StaticAccessItem{
			Kind: "static_file", StaticID: item.StaticID, StaticRef: item.StaticRef, FlowPath: item.FlowPath,
			RelativePath: item.RelativePath, ContentDigest: item.ContentDigest,
			SizeBytes: len(item.Content), ContentType: item.ContentType, MountPath: item.MountPath,
			Content: append([]byte(nil), item.Content...),
		}
		list.Items = append(list.Items, durabledata.AccessItem{Kind: "static_file", Static: &static})
	}
	refs := flowdata.AllowedResourceData(source, actor)
	if len(refs) > 0 {
		if store == nil {
			return durabledata.AccessList{}, fmt.Errorf("durable data selected-store reader is required for declared resource access")
		}
		resources, err := store.LoadRunResourceAccess(ctx, runID, refs)
		if err != nil {
			return durabledata.AccessList{}, err
		}
		if len(resources) != len(refs) {
			return durabledata.AccessList{}, fmt.Errorf("durable data access projection is incomplete")
		}
		for index := range resources {
			resource := resources[index]
			list.Items = append(list.Items, durabledata.AccessItem{Kind: "resource", Resource: &resource})
		}
	}
	sort.Slice(list.Items, func(i, j int) bool { return accessItemPath(list.Items[i]) < accessItemPath(list.Items[j]) })
	for index := range list.Items {
		path := accessItemPath(list.Items[index])
		if path == "" {
			return durabledata.AccessList{}, fmt.Errorf("data access item has no canonical mount path")
		}
		if index > 0 && path == accessItemPath(list.Items[index-1]) {
			return durabledata.AccessList{}, fmt.Errorf("data access projection repeats mount path %s", path)
		}
	}
	return list, nil
}

func accessItemPath(item durabledata.AccessItem) string {
	switch item.Kind {
	case "static_file":
		if item.Static != nil && item.Resource == nil {
			return item.Static.MountPath
		}
	case "resource":
		if item.Resource != nil && item.Static == nil {
			return item.Resource.MountPath
		}
	}
	return ""
}
