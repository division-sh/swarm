# EmpireAI v2.1.0 — Platform Abstraction (Phase A)

**Version:** 2.1.0
**Previous:** 2.0.50
**Date:** 2026-03-07

## Summary

Introduces the MAS orchestration platform specification. The orchestration engine is now described as a generic, reusable platform with EmpireAI as one workflow overlay.

This is Phase A: define the platform spec. Phase B (extract Empire overlay into platform-format files) and Phase C (verify platform + overlay = v2.0.50 behavior) follow.

## New File

### contracts/platform/platform-spec.yaml

Defines the contract formats, vocabulary, and compliance rules for any multi-agent workflow:

- **6 vocabulary primitives:** stage, transition, guard, action, timer, participant (3 types: system_node, agent, runtime)
- **8 contract formats:** workflow_definition, hook_registry, node_definition, event_catalog, agent_registry, tool_registry, policy_definition, prompt_contract
- **Workflow state model:** 9 fields with DDL for workflow_instances table
- **5 built-in guards + 5 built-in actions:** platform hooks available to all workflows
- **16 compliance rules** across 6 categories: graph structure, hook resolution, participant existence, event consistency, node coverage, wiring
- **Versioning model:** platform version (0.1.0) independent of workflow version
- **File layout convention:** contracts/platform/ + contracts/{workflow_name}/

## Spec Changes

- §5.10 added: Platform Abstraction section with contract format table and compliance overview

## What Did NOT Change

All v2.0.50 contracts are unchanged. The platform spec is additive — it defines how the existing contracts should be structured, not what they contain. EmpireAI's workflow-schema.yaml, guard-action-registry.yaml, etc. are valid instances of the platform formats.

## Next Phases

- Phase B: Rename/reorganize EmpireAI contracts to follow platform file layout (contracts/empire/)
- Phase C: Verify platform + overlay = exact v2.0.50 behavior
- Phase D: Implement platform workflow engine that reads generic contracts
