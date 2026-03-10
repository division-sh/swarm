# EmpireAI v2.2.2 — File Reorganization (Phase B)

**Version:** 2.2.2
**Previous:** 2.2.1
**Date:** 2026-03-08

## Summary

Reorganizes contract files from flat directory into platform/empire split. No logic changes. Enables two independent spec work streams: platform (workflow-agnostic) and Empire (business-specific).

## New Directory Structure

```
contracts/
  platform/                          # Workflow-agnostic
    platform-spec.yaml               # Vocabulary, formats, compliance rules
    tools-platform.yaml              # 2 universal tools (agent_message, mailbox_send)
    ddl-platform.sql                 # Platform tables (workflow_instances)
    tooling.lock                     # Version tracking
  empire/                            # EmpireAI workflow overlay
    workflow-empire.yaml             # 18 stages, 27 transitions, 5 timers
    hooks-empire.yaml                # 22 guards, 13 actions
    nodes-empire.yaml                # 5 system nodes, 47 event handlers
    events-empire.yaml               # 172 events
    agents-empire.yaml               # 28 agents
    tools-empire.yaml                # 19 per-agent tool schemas
    policy-empire.yaml               # 44 template variables
    ddl-empire.sql                   # 38 Empire tables
    prompts/                         # 20 agent prompt files
    + changelogs, guide, manifest, config map
  verification-gates.yaml            # 58 gates (shared)
```

## File Mapping

| Old Name | New Path |
|----------|----------|
| workflow-schema.yaml | empire/workflow-empire.yaml |
| guard-action-registry.yaml | empire/hooks-empire.yaml |
| system-nodes.yaml | empire/nodes-empire.yaml |
| event-catalog.yaml | empire/events-empire.yaml |
| agent-tools.yaml | empire/agents-empire.yaml |
| prompt-variables.yaml | empire/policy-empire.yaml |
| ddl-canonical.sql | empire/ddl-empire.sql |
| tool-schemas.yaml | SPLIT: platform/tools-platform.yaml (2 universal) + empire/tools-empire.yaml (19 per-agent) |
| platform-spec.yaml | platform/platform-spec.yaml |
| tooling.lock | platform/tooling.lock |

## New Files

- `platform/ddl-platform.sql` — Platform-level DDL (workflow_instances). Other platform tables flagged for future extraction.
- `platform/tools-platform.yaml` — Universal tools extracted from tool-schemas.yaml.

## Migration for Implementer

```bash
mkdir -p contracts/platform contracts/empire/prompts

# Platform
mv contracts/platform-spec.yaml contracts/platform/
mv contracts/tooling.lock contracts/platform/
# Split tool-schemas.yaml → platform + empire (from tarball)

# Empire  
mv contracts/workflow-schema.yaml contracts/empire/workflow-empire.yaml
mv contracts/guard-action-registry.yaml contracts/empire/hooks-empire.yaml
mv contracts/system-nodes.yaml contracts/empire/nodes-empire.yaml
mv contracts/event-catalog.yaml contracts/empire/events-empire.yaml
mv contracts/agent-tools.yaml contracts/empire/agents-empire.yaml
mv contracts/prompt-variables.yaml contracts/empire/policy-empire.yaml
mv contracts/ddl-canonical.sql contracts/empire/ddl-empire.sql
mv contracts/prompts/ contracts/empire/prompts/
mv contracts/agent-config-map.yaml contracts/empire/
mv contracts/prompt-manifest.sha256 contracts/empire/
mv contracts/upgrade-actions.yaml contracts/empire/
mv contracts/spec-writer-guide.md contracts/empire/
mv contracts/CHANGELOG-*.md contracts/empire/

# Update Go import paths
grep -rl 'contracts/' internal/ cmd/ | xargs sed -i \
  -e 's|contracts/event-catalog.yaml|contracts/empire/events-empire.yaml|g' \
  -e 's|contracts/agent-tools.yaml|contracts/empire/agents-empire.yaml|g' \
  -e 's|contracts/system-nodes.yaml|contracts/empire/nodes-empire.yaml|g' \
  -e 's|contracts/workflow-schema.yaml|contracts/empire/workflow-empire.yaml|g' \
  -e 's|contracts/guard-action-registry.yaml|contracts/empire/hooks-empire.yaml|g' \
  -e 's|contracts/prompt-variables.yaml|contracts/empire/policy-empire.yaml|g' \
  -e 's|contracts/tool-schemas.yaml|contracts/empire/tools-empire.yaml|g' \
  -e 's|contracts/ddl-canonical.sql|contracts/empire/ddl-empire.sql|g' \
  -e 's|contracts/platform-spec.yaml|contracts/platform/platform-spec.yaml|g'
```

## Touches

- platform/platform-spec.yaml: file_layout updated, migration_note → "Phase B complete"
- empire/spec-writer-guide.md: §10 file locations updated with new structure
- empire/workflow-empire.yaml: internal cross-references updated
- empire/hooks-empire.yaml: references to policy file updated
- All version stamps bumped to 2.2.2
