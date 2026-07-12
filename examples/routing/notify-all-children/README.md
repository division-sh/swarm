# Notify All Children

This recipe notifies every known child flow instance without giving the producer delivery-cardinality authority.

The `portfolio` flow owns the ordered `account_ids` membership list as journaled entity state. Its `fan_out` expands that list into one ordinary, targetless `account.notify.requested` event per item. The output pin carries `account_id`, parent `connect` owns the cross-flow topology, and the `account` input uses `resolution.mode: select` to resolve exactly one existing child.

The setup path is production-shaped: `portfolio.opened` creates the owner, `portfolio.account.register.requested` creates account instances through `select-or-create`, and `portfolio.membership.seeded` writes the owner list through a normal event handler. Tests and examples must not seed runtime stores directly.

List order and duplicates are preserved. Expansion does not silently deduplicate. A stale key fails only that item's route with `platform.target_unreachable`; valid siblings still process. The owner can consume the failure event and remove stale membership through ordinary domain logic.

Need to know every child received the notification? Pair the downward fan-out with a join over acknowledgment events, as described by issue #1848. Fan-out delivery itself is deliberately non-atomic across instances.

True one-event delivery to multiple runtime-discovered instances is a different, evidence-gated feature tracked by issue #1934.
