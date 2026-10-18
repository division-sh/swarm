# Native two-transaction control (diagnostic only)

Gate: reviewer-g 6253304321. Base ce08cbfe4e. No production repair, hosted
dispatch, timeout change, retry or new product authority port.

One root, TestSavedProofRebindStandingLockOrderDiagnosticBothStores: invariant is
the native saved-proof rebind/standing lock order and exact rollback readback.
It is a disposable diagnostic root, not a new landing/qualification obligation.

Authentic setup follows sessionprovider/standing_responsibility_test.go and its
existing activeInputFixture: genuine SDK peer/connected session, captured native
claim, operator confirmation, normal saved-proof materialization and activation.
The original parent is completed through AdvanceChannelOnboarding. A separate
reserved rebind parent/account/connection obtains a real SDK admission from
newSessionAuthorityOwner, not an account struct substituted for authority.
Unresolved proof responsibility must be empty and the retained proof active.

storetest.StartChannelCycleDiagnostic composes an additional selected native
owner at the fixture's supplied original location using the existing
NewPostgresStoreForTest seam. Its driver interception follows callback_lock_test;
it returns no raw pool, transaction or SQL callback. Schema bootstrap is through
the existing storetest owner. The native principal FK is checked in pg_constraint
before execution. Private before/after row images are compared, never logged.

Original acquisition correspondence:

- BeginChannelBinding -> operatorchannel.beginBinding -> loadBinding FOR UPDATE.
  Interception blocks only after native driver Rows.Next returned the binding
  tuple. The unchanged next INSERT obtains its original principal FK key-share.
- SessionStandingBindingCurrent -> sessionStandingBindingCurrent ->
  LockPrincipalTx -> LockCurrentPrincipalTx FOR UPDATE. Interception blocks only
  after its native principal tuple returns. Its unchanged next binding query is
  also marked. Different predecessor/replacement parents are required.
- After both acquisition milestones, release the barriers once, accept either
  40P01 victim, join both workers, retain both bounded error graphs and the parsed
  detector cycle. No Error() or error prose/native IDs are printed by the capture.
- Compare complete binding, identity and parent row images. Permit exactly one
  new, exact awaiting-claim rebind identity only if its transaction committed.
  Any victim's partial identity must be absent; all old rows stay byte-exact.

The control uses the existing public journey's 5-second bounded context for the
scheduled region; no product deadline is altered. If setup or an earlier lock
prevents both milestones, return the finite boundary and stop, not a manufactured
replacement cycle. PostgreSQL count 1 with race is first. SQLite owner parity is
sequential and uses the same native admission/currentness and typed row checks;
there is no attempted PostgreSQL two-writer schedule on SQLite.

The parser test copy is generated exactly from the approved pure capture source
by sync-native-cycle-parser.rb; it is not a second implementation. Original Retry
and site capture are unchanged. No conclusion about the hosted participants is
made before comparing the native result to the source map and hosted evidence.
