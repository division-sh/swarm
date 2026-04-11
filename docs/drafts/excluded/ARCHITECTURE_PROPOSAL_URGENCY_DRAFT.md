# Architecture Proposal Urgency Draft

Status: parked draft

Purpose:
- capture a lightweight structure for implementer architecture proposals without adding more heavy prose requirements right now
- revisit later if short-term fixes continue to crowd out high-ROI structural follow-up work

Problem to solve:
- architectural follow-up ideas are often mentioned in PRs or reviews, but the tracking strength is inconsistent
- when the guidance is too heavy, compliance quality drops
- when the guidance is too light, "better direction" becomes non-actionable prose

Draft direction:
- keep the implementer-facing structure short
- force explicit disposition rather than open-ended commentary
- separate current-state reasoning from longer-term structural recommendation

Candidate minimal fields:
- `Current constraint`: what in the current code/state made the short-term fix reasonable
- `Architectural proposal`: the cleaner structural move
- `Implication of this PR`: coupling, assumption, or limitation preserved by the patch
- `Urgency`: `U0 same PR`, `U1 next wave`, `U2 backlog`, `U3 opportunistic`
- `Tracking decision`: `implemented now`, `new issue`, `existing issue`, `watchlist only`, or `declined`

Candidate rules:
- require this only for nontrivial semantic/runtime/parity/failure-class work
- keep it short; no essay requirement
- if urgency is `U0` or `U1`, do not allow prose-only tracking
- if `watchlist only` or `declined`, require a one-line reason

Current concern:
- implementer guidelines are already getting heavy
- any future adoption should prefer PR-template fields or a short checklist over more long-form guideline prose

Example shape:

```text
Architecture Proposal

Current constraint:
- startup proof currently relies on existing visible tool surface

Architectural proposal:
- introduce an explicit probe-safe startup capability/tool

Implication of this PR:
- boot success still depends on incidental `{}`-safe ordinary tools

Urgency:
- U1 next wave

Tracking decision:
- new issue
```

Open question for later:
- whether this belongs in implementer guidance, PR template, reviewer checklist, or only lead/review close-out
