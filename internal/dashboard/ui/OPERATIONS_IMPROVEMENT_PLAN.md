# Operations Improvement Plan

## Current State

The dashboard already has the core business logic needed for a serious operations surface:

- mailbox summaries and mailbox decision flows
- human task lists, task stats, and task mutation flows
- control-target actions for runtime and agent recovery
- downstream pivots from `Agents` and `Portfolio`

The main gap is product synthesis, not backend state. `Operations` is still a merged wrapper around `ControlView` and `TasksView`, so mailbox and human intervention flows feel like separate tools.

## Target Model

`Operations` should become the human intervention console.

The target user questions are:

1. What needs human attention right now?
2. Is this a mailbox decision or a human task?
3. What vertical or agent is blocked?
4. What is the fastest safe action to take?

## Phased Plan

### Phase 1: Shared Triage Layer

- add a shared operations summary above mailbox and tasks
- add a unified `Needs Action` queue spanning:
  - pending / critical mailbox items
  - open / review / overdue tasks
- add current focus context for the selected mailbox item or task
- add quick pivots into:
  - mailbox decisions
  - task execution

Status: complete

### Phase 2: Task UX Upgrade

- replace raw task stats JSON with structured cards
- add urgency-first task sections
- improve selected-task workflow guidance
- make task actions more explicit about outcome and follow-up

Status: complete

### Phase 3: Mailbox UX Upgrade

- make mailbox items summary-first
- add clearer request metadata and human decision framing
- reduce raw JSON-first presentation
- show related vertical/task context when available

Status: complete

### Phase 4: Unified Human Work Queue

- add a dedicated `Needs Action` subview or panel
- sort mailbox and tasks by urgency across both systems
- show blocked vertical / agent / workflow context in one place

Status: complete

### Phase 5: Workbench Evaluation

- once the triage and unified queue are strong, evaluate whether `Operations` should move to a Dockview workbench like `Workflow` and `Portfolio`
- only do this if the surface needs more than:
  - `Triage`
  - `Control`
  - `Tasks`

Status: complete

## Execution Order

1. Phase 1: shared triage layer
2. Phase 2: task UX upgrade
3. Phase 3: mailbox UX upgrade
4. Phase 4: unified human work queue
5. Phase 5: workbench evaluation

## Scope Boundaries

- no backend API changes required for phases 1-3
- cross-linking between mailbox and tasks should use existing task/mailbox/vertical fields where possible
- if richer mailbox-task linkage is needed later, that can be added as a backend enhancement after the first product pass
