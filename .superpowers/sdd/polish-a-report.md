# Polish-A Report

**Status:** All three fixes applied and verified. Commit made.

**Commit:** `065c0e52` — "Close deferred minors: terminal-state + ListNewest resolved-filter coverage, reword deadline SQL comment"

## Conformance PASS — all 3 drivers

- `TestDriverRiverPgxV5/DefaultMode/(JobGetWorkflowDeadlineExpired|WorkflowSignal)`: **14 passed** ✓
- `TestDriverRiverDatabaseSQL/DefaultMode/(JobGetWorkflowDeadlineExpired|WorkflowSignal)`: **2 passed** ✓
- `TestDriverRiverSQLite/DefaultMode/(JobGetWorkflowDeadlineExpired|WorkflowSignal)`: **1 passed** ✓

## make verify/sqlc

Clean — no diff (3 drivers: riverdatabasesql, riverpgxv5, riversqlite all pass `sqlc diff`).

## riverworkflow suite

`go test ./...` in `riverworkflow/`: **94 passed** across 4 packages.

## Concerns

- Pre-existing `JobKindList`/`QueueNameList` failures in `ExecMode`/`SimpleProtocol` pgx modes are unrelated to this work (explicitly excluded in the task spec).
- SQLite test report shows "1 passed in 1 packages" because it matches only 1 top-level test function (`TestDriverRiverSQLite`) — the 276 subtests within it all pass when run with `-run TestDriverRiverSQLite` alone.
