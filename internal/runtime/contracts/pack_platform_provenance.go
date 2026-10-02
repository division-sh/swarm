package contracts

import (
	"path"
	"strconv"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func populatePackPlatformProvenance(bundle *WorkflowContractBundle, builder *effectiveProvenanceBuilder) error {
	add := func(prefix string, value yamlsource.Value, packID string) error {
		if value.Presence() == yamlsource.PresenceMissing {
			return nil
		}
		entries := map[string]EffectiveValueProvenance{prefix: authoredSourceProvenance(value)}
		if err := collectNodeValueProvenance(value, prefix, entries, nil); err != nil {
			return err
		}
		for path, entry := range entries {
			entry.PackIdentity = packID
			builder.set(path, entry)
		}
		return nil
	}
	root := bundle.Platform.SourceValue()
	if root.Presence() != yamlsource.PresenceMissing {
		for _, name := range []string{"platform", "interfaces", "permissions_model", "workflow_state", "platform_tables", "builtin_hooks"} {
			lookup, err := root.Lookup(name)
			if err != nil {
				return err
			}
			if err = add("platform."+name, lookup.Value, ""); err != nil {
				return err
			}
		}
		for _, path := range [][]string{{"vocabulary", "participant", "types"}, {"platform_events", "catalog"}} {
			value := root
			for _, name := range path {
				lookup, err := value.Lookup(name)
				if err != nil {
					return err
				}
				value = lookup.Value
				if value.Presence() == yamlsource.PresenceMissing {
					break
				}
			}
			if value.Presence() == yamlsource.PresenceMissing {
				continue
			}
			if path[0] == "vocabulary" {
				if err := add("platform.vocabulary.participant.types", value, ""); err != nil {
					return err
				}
				continue
			}
			rows, err := value.Mapping()
			if err != nil {
				return err
			}
			for _, row := range rows {
				prefix := "platform.events[" + strconv.Quote(row.Name) + "]"
				builder.set(prefix, authoredSourceProvenance(row.Value))
				payload, err := row.Value.Lookup("payload")
				if err != nil {
					return err
				}
				if err := add(prefix+".payload", payload.Value, ""); err != nil {
					return err
				}
			}
		}
	}
	if err := add("packs.project_membership", bundle.ProjectPacks.SourceValue(), ""); err != nil {
		return err
	}
	if bundle.PackInventory != nil {
		if bundle.PackInventory.BaseSelectionMode() == packartifact.SelectionDevelopmentOverride {
			builder.set("packs.platform_membership", EffectiveValueProvenance{
				Origin: EffectiveValueOriginBoundarySnapshot, RuleID: "pack.complete_development_inventory",
			})
		} else {
			if err := add("packs.platform_membership", bundle.PackInventory.SourceValue(), ""); err != nil {
				return err
			}
		}
		for _, entry := range bundle.PackInventory.Entries() {
			prefix := "packs[" + strconv.Quote(entry.ID()) + "].envelope"
			if err := add(prefix, entry.Envelope().SourceValue(), entry.ID()); err != nil {
				return err
			}
			builder.set("packs["+strconv.Quote(entry.ID())+"].body", EffectiveValueProvenance{
				Origin: EffectiveValueOriginBoundarySnapshot, RuleID: "pack.admitted_body_bytes", PackIdentity: entry.ID(),
				SourceFile: path.Join(entry.Directory(), packartifact.ManifestFileNameForType(entry.Type())),
			})
			if entry.Source() == packartifact.ProvenanceProject {
				builder.set(prefix+".effective_manifest_hash", EffectiveValueProvenance{Origin: EffectiveValueOriginDerived, RuleID: "pack.manifest_body_digest", PackIdentity: entry.ID(), InputPaths: []string{prefix + ".manifest_hash", "packs[" + strconv.Quote(entry.ID()) + "].body"}})
			}
		}
	}
	if bundle.PackAdmission != nil {
		identities := map[string]string{}
		if bundle.PackInventory != nil {
			for _, entry := range bundle.PackInventory.Entries() {
				identities[entry.ID()+"."+entry.Type()] = entry.ID()
				identities[entry.ID()+".generated_profile"] = entry.ID()
			}
		}
		for name, value := range bundle.PackAdmission.PackSourceValues() {
			if err := add("pack_sources["+strconv.Quote(name)+"]", value, identities[name]); err != nil {
				return err
			}
		}
	}
	return nil
}
