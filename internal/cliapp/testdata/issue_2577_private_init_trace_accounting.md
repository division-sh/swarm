# #2577 Private Initialization Trace Accounting

The admission predicate generator changes only the body fingerprints of
serveSessionBootstrap.openSessionAttempt and completeSessionAttempt. Both
changes add standard runtime/trace regions; the request cancellation branch
adds a constant diagnostic event. No admission branch, context transfer,
cleanup/join, observation classification or execution capability changes.

The same named owners, audit rows, proof families and census scanner remain.
The compiled describe corpus regenerates without a delta. No schema/API or
predicate inventory is removed to accommodate the private initialization fix.
