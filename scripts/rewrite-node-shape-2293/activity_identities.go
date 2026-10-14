package main

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type activityIdentityPin struct {
	Bundle         string                         `json:"bundle"`
	BundleHash     string                         `json:"bundle_hash"`
	Flow           string                         `json:"flow"`
	Node           string                         `json:"node"`
	Handler        string                         `json:"handler"`
	Site           string                         `json:"site"`
	File           string                         `json:"file"`
	Line           int                            `json:"line"`
	Column         int                            `json:"column"`
	Explicit       bool                           `json:"explicit"`
	Approval       bool                           `json:"approval"`
	ResultIdentity contracts.ActivityResultEvents `json:"result_identity"`
}

func captureActivityIdentities(repo string, roots []string) ([]activityIdentityPin, error) {
	if len(roots) == 0 {
		return nil, fmt.Errorf("select source bundle roots explicitly")
	}
	pins := []activityIdentityPin{}
	for _, root := range roots {
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", root, err)
		}
		bundle, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", root, err)
		}
		for _, site := range bundle.ActivitySites() {
			provenance, ok := bundle.HandlerValueProvenance(site.Node, site.HandlerEventKey, strings.TrimPrefix(site.Source, "handler."))
			if !ok {
				return nil, fmt.Errorf("%s: activity %s/%s/%s has no admitted source coordinate", root, site.Node.NodeID(), site.HandlerEventKey, site.Source)
			}
			pins = append(pins, activityIdentityPin{
				Bundle: root, BundleHash: artifact.BundleHash(),
				Flow: site.Node.FlowPath(), Node: site.Node.NodeID(), Handler: site.HandlerEventKey, Site: site.Source,
				File: provenance.SourceFile, Line: provenance.SourceLine, Column: provenance.SourceColumn,
				Explicit: strings.TrimSpace(site.Spec.ID) != "", Approval: site.Spec.Approval != nil,
				ResultIdentity: contracts.ActivityResultEventsForSite(site),
			})
		}
	}
	return pins, nil
}
