# M3 Engine Harvest Audit

**Date:** 2026-03-12  
**Status:** Architectural Guidance for Phase 2, Step 2/3  
**Source File:** `internal/runtime/pipeline/handler_engine_exec.go`

## Goal
Identify the logic that can be "harvested" (copy-pasted or adapted) from the legacy `pipeline` handlers into the new generic `internal/runtime/engine` package to ensure behavioral parity and minimize rewrite risk.

---

## 1. The Harvest Map (12-Step Alignment)

| Step | Operation | Legacy Logic Location | Action |
| :--- | :--- | :--- | :--- |
| **1** | `clear_gates` | `exec.clearGates()` (Lines 415-430) | **Adapt:** Remove hardcoded gate names (`g1_research`). Make generic. |
| **2** | `guard` | `exec.evaluateGuard()` (Lines 240-295) | **Harvest:** The recursive logic for `GuardSpec` is stable. |
| **3** | `accumulate` | `exec.accumulate()` (Lines 297-332) | **Harvest:** The `handlerEngineAccumulator` struct and arrival logic. |
| **4** | `compute` | `exec.compute()` (Lines 334-358) | **Harvest:** Use helpers `computeValue`, `weighted_average`, etc. (Lines 520-645). |
| **5** | `fan_out` | `exec.fanOutItems()` (Lines 360-386) | **Adapt:** Use new `EmitIntent` and `OutboxWriter` interfaces. |
| **6** | `on_complete` | `exec.onComplete()` (Lines 388-408) | **Harvest:** The branching logic for completion rules. |
| **7** | `rules` | `exec.applyRules()` (Lines 495-518) | **Harvest:** First-match rule list logic. |
| **8** | `advances_to` | `exec.advanceState()` (Lines 410-438) | **Rewrite:** Implement **Implicit Timers** (Constraint #4) here. |
| **9** | `sets_gate` | `exec.setGate()` (Lines 440-454) | **Harvest:** Metadata update logic. |
| **10** | `data_writes` | `exec.accumulateData()` (Lines 456-473) | **Harvest:** Use `applyWorkflowDataAccumulationToState` (Lines 715-745). |
| **11** | `transform` | `exec.payloadTransform()` (Lines 660-683) | **Harvest:** Path-based payload mapping logic. |
| **12** | `emits` | `exec.emitEvents()` (Lines 475-493) | **Rewrite:** Implement **Outbox Semantics** and **Chain Depth** (Constraints #3, #5). |
| **13** | `action` | `exec.executeAction()` (Lines 495-535) | **Harvest:** Built-in actions like `record_evidence`. |

---

## 2. Critical "Parity Traps" (Do Not Miss)

These are hidden behavioral logic that will cause silent failures or regressions if changed.

### A. Expression Prefixing (The `vars.` Rule)
*   **Logic:** `rewriteHandlerExpression` (Lines 1083-1089).
*   **Requirement:** Any expression starting with `metadata.`, `accumulated.`, or `fan_out.` MUST be prefixed with `vars.` before being sent to the CEL evaluator.

### B. Arrival Identifier Priority
*   **Logic:** `arrivalIdentifier()` (Lines 1051-1065).
*   **Requirement:** The priority list for identifying a sender (`SourceAgent` -> `payload.source` -> `payload.from` -> etc.) must be preserved exactly.

### C. State Bucket JSON Paths
*   **Logic:** `loadAccumulator()` (Lines 828-850).
*   **Requirement:** Persistence MUST use the exact key `bucket["handler_accumulators"][handlerKey]` in the Postgres JSONB blob.

### D. Variable Resolution Logic
*   **Logic:** `resolveRef` and `resolveRefOrLiteral` (Lines 647-713).
*   **Requirement:** This is the "Brain" of the context builder. It must handle `entity.`, `payload.`, `policy.`, and `metadata.` resolution exactly as the current system does.

## 3. High-Risk "Shadow Logic" (Spec v1.1.0 Alignment)

The following logic was identified during the Implementer Audit and MUST be addressed via the **Adapter Seam** or **Contract Migration** strategies. The new engine will enforce the **v1.1.0 Platform Spec** strictly.

### A. Strict Single-Authority Model
*   **Decision:** **ENFORCE AT BOOT.** Every event MUST be owned by exactly one Authoritative System Node. The legacy "silent drop" of ambiguous events is replaced with a **Boot Error**.
*   **Execution:** The 12-step executor only processes one node handler per transaction. Reactive agents receive events in parallel **post-commit**.

### B. Generic Clear_Gates
*   **Decision:** **PLATFORM-DRIVEN.** Do NOT port the hardcoded "Empire" gate list. `clear_gates: true` must reset all gates defined in the flow's `schema.yaml`.

### C. Payload Shaping Seam
*   **Decision:** **ADAPTER SEAM.** `handlerEmitPayload` is a product-specific factory. Implement a `PayloadShaper` interface in the engine to delegate this to the `pipeline` adapter.

### D. Guard Magic Fallback
*   **Decision:** **REJECT AT BOOT.** Legacy "Guard ID as CEL Expression" is deprecated. All YAML contracts must be migrated to use explicit `check:` fields. No "guessing" in the new engine.

### E. Rule Exclusivity
*   **Decision:** **REJECT AT BOOT.** `on_complete` and `rules` are mutually exclusive. Handlers using both are an error. Refactor conflicting Empire contracts as part of M3.

### F. Product Builtin Isolation
*   **Decision:** **REGISTRY ISOLATION.** Product-era builtins (e.g., `revision_count_below_limit`) must be moved to the `GuardRegistry`. The engine will only see the ID and delegate the Go execution to the product adapter.

### G. Implicit Audit & Timers
*   **Decision:** **IMPLICIT ENGINE ACTIONS.** Per Spec v1.1.0, the 4 implicit actions (`record_state_change`, `update_stage`, `cancel_stage_timers`, `start_stage_timers`) MUST be hardcoded into the executor's Step 13. They only execute if the state actually changes.

### H. Chain Depth & Deduplication
*   **Decision:** **SPEC-DRIVEN.** 
    *   **Chain Depth:** Enforce `max_depth: 50` at Step 12 (Emits). 
    *   **Deduplication:** Update `accumulate` to use the new `dedup_by` contract field for arrival tracking.

---

## 4. Implementation Guardrails


1.  **Strict Transactionality:** Ensure the `TransactionRunner` in the new engine wraps all 12 steps.
2.  **Implicit Timers:** Move the timer start/cancel logic from the `coordinator` directly into the `advances_to` handler in the `executor`.
3.  **Chain Depth:** Add a `ChainDepth` check to the `emits` step. If `depth > 50`, move the event to the dead-letter queue.

---
**This document serves as the Technical Reference for the M3 Engine Implementation.**
