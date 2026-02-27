# Spec Writer Agent: Lessons Learned

**Context:** EmpireAI system architecture specification, v2.0.22 → v2.0.34, written across 7 sessions over ~20 hours. Two external review cycles. One implementer building from the spec. Spec grew from ~10K to ~13.5K lines with 5 machine-readable contract files.

This document captures everything a future spec writer agent should know before touching this spec (or any large multi-agent system spec). These aren't hypothetical — every lesson comes from a real mistake that cost a review cycle or confused the implementer.

---

## 1. The Spec Is Not the Product — The Contracts Are

### The hard lesson

I spent months writing beautiful prose describing agent behaviors, event flows, and database schemas. Then we extracted machine-readable contracts (YAML, SQL) from that prose and immediately found **33 phantom events, 5 wiring gaps, and 7 wrong table schemas**. The prose had drifted from itself — different sections described the same thing differently, and nobody noticed because prose is hard to cross-validate.

### The rule going forward

**Contracts are the source of truth. Prose explains why; contracts define what.**

The spec ships 5 contract files:

| File | What it defines | What it replaces |
|------|-----------------|------------------|
| `agent-tools.yaml` | Every agent's subscriptions, tools, emit_events | §4.2.2 agent table, Appendix B configs |
| `event-catalog.yaml` | Every event's emitter, consumer, delivery_channel, payload | §5.4 event catalog, §5.7.1 producer registry |
| `ddl-canonical.sql` | Every table's exact columns, types, constraints | §8.1 DDL, migration descriptions |
| `upgrade-actions.yaml` | Typed actions per version with verify commands | Changelog prose "implementation actions" |
| `verification-gates.yaml` | Required test gates with pass/fail criteria | Ad-hoc verification instructions |

**When you edit the spec, edit the contract file FIRST, then update prose to match.** Not the other way around. This is the single most important workflow change.

### Why this matters mechanically

The implementer loads YAML directly into cross-validation scripts. If the YAML is wrong, the implementer builds wrong code. If the prose is wrong but YAML is right, the implementer still builds correct code. Prose errors are annoying; contract errors are destructive.

---

## 2. Never Invent Schemas — Derive Them from Go Structs

### The catastrophic mistake

In v2.0.32, I was asked to add 7 missing runtime tables to `ddl-canonical.sql`. I knew these tables *existed* conceptually (pipeline_receipts, scan_accumulators, validation_pipelines, etc.) but instead of finding the Go struct definitions already in the spec and translating them to DDL, I **designed idealized schemas from scratch**.

The result: `validation_pipelines` got a row-per-stage design when the Go struct uses boolean gate flags (G1-G4). `scan_accumulators` got integer counters when the Go struct uses JSONB arrays. `pipeline_receipts` got a multi-handler composite key when the code uses a simple event_id PK. **Every single query in the Go code would have failed against my DDL.**

The reviewer scored the DDL at 5/10 — *lower* than the 6/10 before I added the tables. Adding wrong schemas was worse than having no schemas.

### The rule

When creating DDL for a table:

1. **Find the Go struct first.** `grep "type TableName struct" empireai-*.md`
2. **Translate field-by-field.** Go `bool` → Postgres `BOOLEAN`. Go `json.RawMessage` → Postgres `JSONB`. Go `uuid.UUID` → Postgres `UUID`. Go `string` → Postgres `TEXT`.
3. **Check the code that reads/writes the table.** The `RecoverFromCrash()` query tells you the real PK. The `writePipelineReceipt()` call tells you the real columns.
4. **Never add columns the Go code doesn't reference.** If the struct has 8 fields, the table has 8 columns (plus created_at/updated_at). Not 12. Not 15.

If there's no Go struct for a table, that's a spec gap — write the struct first, THEN the DDL.

**Column names must be exact.** `g2_spec` is not the same as `g2_spec_approved`. `expected` is not the same as `expected_agents`. `status` + `error` is not the same as `result`. Go's `db.Scan()` reads columns by name — a single character difference is a runtime crash. When the reviewer gives you the correct column names, use them verbatim. Don't "improve" them.

---

## 3. Changelogs Are Not Optional — And "Title Only" Is Worse Than Nothing

### The mistake

v2.0.32 was the most substantial revision — it fixed 34 missing events, 7 missing tables, wildcard subscriptions, misattributed agents. Its changelog was title-only: `**v2.0.32 — Agent-Tools Reconciliation, Missing Events/Tables, DDL Completeness**` followed by... nothing. No narrative, no implementation actions.

The reviewer caught this immediately: "The most substantial revision has zero documentation."

### The rule

Every version gets a full changelog with TWO sections:

```markdown
### Spec changes (narrative)
- What changed, why, what it fixes, what the impact is

### Implementation actions (mechanical)
- ADD: file/path — description
- EDIT: file — specific change (key_path if applicable)
- MIGRATE: SQL statement
- DROP: what to remove
- GREP-AND-KILL: pattern → replacement
- VERIFY: command + expected result
```

The narrative is for humans understanding context. The implementation actions are for the implementer's checklist. Both are required. If you're too tired to write the changelog, you're too tired to commit the version — stop and write it before bumping.

Additionally: the `upgrade-actions.yaml` contract file now provides machine-readable actions. Future versions should have BOTH: prose changelog in the spec AND machine-readable actions in the contract. They serve different audiences.

---

## 4. Ghost Removal Requires Grep-Level Thoroughness

### The pattern

The Scoring Coordinator was removed in v2.0.19. By v2.0.34, it had survived **six review cycles** as ghost references. Each cycle, I'd remove the ones the reviewer found, and miss others. The SC appeared in:

- Active agent tables (removed v2.0.27)
- ASCII diagrams (removed v2.0.34 — survived 7 versions!)
- §16 directory tree as `scoring/coordinator.go` (removed v2.0.34)
- Model tier tables, roster lists, seed data
- Event schema emitter fields
- Changelog entries (correctly preserved — these are historical)

### The rule

When removing an agent (or any major component):

1. `grep -rni "component_name" empireai-*.md` — get every reference
2. Classify each as **active** (must remove) or **historical** (must preserve)
3. Remove all active references in one atomic version
4. Add a VERIFY step: `grep -ri "component_name" internal/ configs/ | grep -v changelog | wc -l → 0`
5. Check ASCII diagrams manually — grep doesn't catch box-drawing characters

The rule applies to any removal: agents, events, tables, config fields. If you remove something, prove it's fully removed.

---

## 5. Prose and Contracts Drift — Cross-Validate Every Version

### The problem

Section A says Discovery Coordinator emits `scan.completed`. Section B says Runtime emits `scan.completed`. The event-catalog.yaml says DC. The Go code says Runtime. Four sources, two answers, and only the Go code is correct (DC was rewritten in v2.0.9 to judgment-only).

This kind of drift is *inevitable* in a 13K-line spec. Every change touches multiple sections, and you will miss at least one.

### The solution

After every batch of spec changes, run cross-validation:

```python
# Load all three contracts
agents = yaml.safe_load(open('contracts/agent-tools.yaml'))
catalog = yaml.safe_load(open('contracts/event-catalog.yaml'))
# Then check:
# 1. Every emit_event has a catalog entry
# 2. Every subscription has a catalog entry
# 3. Every catalog emitter is a real agent or system component
# 4. Every event has delivery_channel
# 5. Consumer subscriptions align with catalog consumers
```

This takes 30 seconds and catches 80% of drift. The implementer does this automatically now — you should too before every version bump.

---

## 6. The Implementer Is Your Real Customer — Not the Reviewer

### What I got wrong

I optimized for reviewer approval: clean prose, comprehensive coverage, architectural elegance. The implementer cares about different things:

- **Can I run `psql -f` and get a working database?** (Not if the schemas are invented)
- **Can I grep for what changed and know exactly what to do?** (Not if the changelog is narrative-only)
- **Can I validate my work mechanically?** (Not if verification is prose instructions)
- **Do file paths in the spec match my repo?** (Not if you use `market-research.yaml` when the convention is `market-research-agent.yaml`)

### What the implementer explicitly asked for

1. **`upgrade-actions.yaml`** — Machine-readable upgrade delta with type, target_file, key_path, expected_before/after, verify_command. Per release.
2. **Normalized naming** — Map contract agent IDs to actual config filenames.
3. **Must-pass vs optional** — Mark DB-live checks distinctly so they can report UNVERIFIED without ambiguity.
4. **`verification-gates.yaml`** — Binary compliance: all must_pass gates green = compliant.

These are all about **reducing interpretation overhead**. The implementer doesn't want to read prose to figure out what to do. They want structured, executable instructions.

---

## 7. The Archive Format Matters

### What works

```
empireai-v2.0.34/
├── empireai-v2_0_34.md           # Spec prose (the why)
├── UPGRADE-GUIDE.md              # Human-readable upgrade steps
├── changelog-actions-checklist.md # Retroactive full checklist
└── contracts/                    # Machine-readable truth
    ├── agent-tools.yaml
    ├── event-catalog.yaml
    ├── ddl-canonical.sql
    ├── upgrade-actions.yaml
    └── verification-gates.yaml
```

Key design decisions:

- **Single tar.gz.** The implementer drops it, extracts, diffs against previous. No chasing files across messages.
- **`contracts/` directory is atomic.** Copy entire directory to repo root. Don't cherry-pick files.
- **UPGRADE-GUIDE.md** is for humans catching up from an older version. Ordered steps with checkboxes.
- **`upgrade-actions.yaml`** is for tools/scripts processing the delta mechanically.
- **Version in filename.** `empireai-v2.0.34.tar.gz` — no ambiguity about what's inside.
- **Spec filename uses underscores** (`empireai-v2_0_34.md`) because some tools choke on dots in filenames.

### What doesn't work

- Pasting spec sections into chat messages (gets lost, no version control)
- Multiple separate file downloads (implementer has to reassemble)
- Sending only changed sections (implementer lacks context)
- Spec without contracts (implementer has to parse prose into data structures)

---

## 8. Review Findings Cascade — Fix Them All At Once

### The pattern

Reviewer finds 10 issues. I fix 7 in the next version. Reviewer finds 3 remaining + 2 new issues caused by my fixes. I fix 4. Reviewer finds 1 remaining + 1 new. This went on for three cycles on some issues (schedule tool missing from 6 agents, SC in ASCII diagram).

### The rule

When a review comes back:

1. List EVERY finding, not just the critical ones
2. Fix ALL of them in one version, not incrementally
3. For each fix, add a VERIFY step that the reviewer can run
4. If you can't fix something, explicitly say "DEFERRED: [reason]" — don't silently skip it

The reviewer's time is expensive. Making them find the same issue three times is wasteful and erodes trust.

---

## 9. Event Catalog Completeness Is Hard — Use Agent Configs As Source

### The discovery

When I extracted `event-catalog.yaml`, I started from the §5.4 prose event table. It had ~85 events. Then I cross-validated against agent configs (emit_events + subscriptions) and found **33 events that agents were emitting or subscribing to but that had no catalog entry**. The prose table was ~72% complete.

### The workflow

To build a complete event catalog:

1. Walk every agent's `emit_events` list → those events must exist in the catalog
2. Walk every agent's `subscriptions` list → those events must exist in the catalog
3. Walk the PipelineCoordinator's interceptor switch cases → those events must exist
4. Walk the runtime's deferred emission patterns → those events must exist
5. Only THEN check the prose table for anything missed

Agent configs are closer to ground truth than prose tables because agents that emit events that don't exist will fail at runtime.

---

## 10. delivery_channel Was the Best Addition — Make Similar Disambiguation Fields

### The problem before

Events like `cto.tech_spec_feedback` had `routing: static` in the catalog but were actually delivered via `agent_message`. The verifier had to read YAML comments to infer delivery mechanism. Comments lie.

### The fix

Added a 6-value `delivery_channel` enum to every event. The verifier reads one field and knows exactly what to check. No inference, no comments, no ambiguity.

### The general principle

Whenever a field requires human interpretation or inference to be useful, add a machine-readable disambiguation field. Examples:

- Event delivery → `delivery_channel`
- Agent subscription type → `subscriptions` vs `subscriptions_bootstrap`
- Action priority → `must_pass` vs `should_fix` vs `optional`
- Table provenance → which Go struct it maps to (consider adding this to DDL comments)

---

## 11. Version Bump Discipline

### The mechanical process

1. Make all spec changes for this batch
2. Edit contract files FIRST if affected (YAML/SQL)
3. Run cross-validation script
4. Write changelog with BOTH narrative + implementation actions
5. Write `upgrade-actions.yaml` entries for this version
6. Update `verification-gates.yaml` if new gates needed
7. Bump version number in spec header
8. Build archive: `tar czf empireai-v{X}.tar.gz empireai-v{X}/`
9. Verify archive extracts cleanly

**Never skip steps 2-6.** The moment you say "I'll write the changelog later," you won't, and the reviewer will catch it.

---

## 12. Specific Pitfalls to Watch For

**Naming inconsistencies:** `prebrand-agent` vs `pre-brand-agent`. Pick one convention and grep for violations after every change. The current convention: `{role}-agent` for factory sub-agents, `opco-{role}` for OpCo agents.

**Glob patterns in subscriptions:** EventBus does not support `opco.*.X`. Every time you write a wildcard subscription, you're writing a bug. Use direct event names.

**agents.id is TEXT, not UUID:** This one caused a real FK type mismatch. Any table referencing agents must use TEXT.

**"Phantom" events:** Events referenced in prose but never emitted or consumed by any agent. The catalog had at least 6 of these. If an event has no emitter AND no consumer in the contracts, it's a phantom — remove it or document why it exists.

**Section numbering:** When you add a new section (like §17 Contracts), renumber everything downstream AND grep for cross-references. `§17.*Open Questions` broke when Open Questions became §18.

**Empty DDL columns:** If a DDL column has a comment like `-- 'pending' | 'assigned' | 'completed'` but no CHECK constraint, add the CHECK. Comments don't prevent bad data.

---

## 13. What the Spec Still Needs (Known Debt)

As of v2.0.34, these issues are tracked but unresolved:

1. **subscriptions_bootstrap data:** 7 of 9 OpCo workers have empty bootstrap entries in the contract but have real subscriptions in configs
2. **schedule + human_task_request tools:** Missing from 6-7 agent entries despite being in configs/prompts
3. **6 missing events** in catalog: opco.routing_updated, customer_message, human_task.assigned, review.product_spec_feedback, review.deploy_feedback, runtime.reset
4. **~8 payload mismatches** between catalog and event schemas
5. **~66% of event schemas** use permissive default template (should be tightened)
6. **§4 is ~195KB** — should be split into subsections for readability
7. **OpCo Support subscription naming** mismatch: contract vs config

---

## 14. For the Human (Spec Owner)

The spec writer agent is good at:
- Systematic cross-validation
- Maintaining contract files
- Catching structural inconsistencies
- Writing changelogs and upgrade guides
- Applying reviewer feedback

The spec writer agent is bad at:
- Knowing what the Go code *actually does* vs what it *should* do (the DDL disaster)
- Prioritizing implementer needs over reviewer approval
- Catching its own blind spots (SC ghosts survived 6 cycles)
- Knowing when to stop adding complexity vs shipping

The best workflow is: **spec agent writes → reviewer validates → implementer tests → feedback loops to spec agent**. Cut the feedback loop short and you get v2.0.32 (33 fixes with no changelog, 7 tables with wrong schemas).
