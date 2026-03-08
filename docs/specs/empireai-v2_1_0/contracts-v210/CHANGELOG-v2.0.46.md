# EmpireAI v2.0.46 — Audit Remediation + Prompt Completeness

**Version:** 2.0.46
**Previous:** 2.0.45
**Date:** 2026-03-05

## Summary

Addresses 24 of 35 findings from the v2.0.45 specification audit. All 4 CRITICALs resolved, 10 of 12 HIGHs resolved, 10 of 14 MEDIUMs resolved.

## Key Fixes

- **scanner-agent model_tier** sonnet → haiku (C1, ~4x cost reduction at scan volumes)
- **signal_strength threshold** reconciled to 55 across system-nodes.yaml, discovery-coordinator prompt, verification gates (C2+H12)
- **8 v2.0.45 upgrade actions** added for typed payloads, prompt calibration, manifest (C3)
- **EC hallucinated emitter** emit_vertical_shortlisted → emit_vertical_resumed for marginal promotion (C4)
- **3 prompt gaps** closed: holding-devops (M3), factory-cto (M4), operations-analyst (M5)
- **Subscription gaps** fixed: pipeline.dead_letter → operations-analyst (H6), validation.more_data_needed → BRA (H8), churn_risk/spend_needed → opco-head-of-product (M8), bug_fix_deployed → opco-support (M9)
- **DDL hardened**: 4 indexes added (H11+M11), budget_envelope NUMERIC → INTEGER (L2)

## Touches

contracts:
  - agent-tools.yaml: scanner-agent model_tier → haiku (C1). pipeline.dead_letter → operations-analyst subs (H6). validation.more_data_needed → BRA subs (H8). churn_risk + spend_needed → opco-head-of-product bootstrap (M8). bug_fix_deployed → opco-support bootstrap (M9).
  - system-nodes.yaml: derivation pre-filter signal_strength → 55 (C2). portfolio.snapshot example → ops.agent_panic (L3).
  - verification-gates.yaml: 7 gate refs v2_0_44 → v2_0_46 (H1).
  - upgrade-actions.yaml: 8 v2.0.45 actions added (C3). previous_version → 2.0.44 (H2). Section header v2.0.44 → v2.0.43 (H3).
  - ddl-canonical.sql: 4 indexes added (H11+M11). budget_envelope NUMERIC → INTEGER (L2).
  - tooling.lock: created (M10).
  - spec-writer-guide.md: prompts/ section 2.8 added, archive structure updated (M12).
  - prompts/empire-coordinator.md: emit_vertical_shortlisted → emit_vertical_resumed (C4). Template migration, human task, budget, OpCo management handlers expanded.
  - prompts/holding-devops.md: spend.approved/rejected, ops.agent_failed handlers + 8 emit tools documented (M3).
  - prompts/factory-cto.md: opco.*.cto_escalation → opco.escalation (H10). 6 missing emit tools documented (M4).
  - prompts/operations-analyst.md: budget.threshold_crossed handler added (M5).
  - prompts/trend-research-agent.md: market_intersection/urgency references removed entirely (H9).
  - prompts/discovery-coordinator.md: signal_strength threshold → 55 (C2).
  - prompt-manifest.sha256: regenerated.
  - CHANGELOG-v2.0.44.md: action count 10 → 11 (L5).

spec:
  - Platform Capability Registry version attribution → v2.0.44 (M6).
  - Version bumped to 2.0.46.

## Post-Update Verification

After copying all contract files from this tarball:

```
cd contracts/prompts && sha256sum -c ../prompt-manifest.sha256
```

All 19 files must show `OK`. Any mismatch means a stale prompt file. This is the #1 cause of runtime schema rejections — do not skip.

## Deferred

  - M1: 10 missing prompt files (OpCo agents + scanner) — fix before OpCo spinup
  - M7/M13/M14: Prompt content validation gate + upgrade-actions coverage gate — implementer building
  - L1: runtime_config unused DDL table — no harm, leave
  - L4: Worker agents empty emit_events — correct by design
