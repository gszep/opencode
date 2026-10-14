# Native session drain v1 (source candidate)

`SessionDrain.begin` and `complete` are Core-only APIs. No HTTP endpoint, generated
client capability, runtime release or managed Continue action exposes them yet.
The candidate extends the current carry without changing native session IDs or
exported history. Build it under a new immutable runtime version, never the
installed `2.0.15-chi.2` label.

## Guarantee and boundary

The request binds a root Session ID, request UUID, native generation (initially
zero) and exact event sequence. `begin` atomically checks that sequence and
persists generation 1's admission fence. It does not interrupt work or claim it
has stopped. `complete` certifies quiescence only when the root and descendants
have no execution claims, queued inputs, unresolved owned-work tickets or
uncertified background markers. Its receipt preserves the original request and
records the final native sequence after settlement. Native sequence is not a Chi
pin: the host still has to synchronize and verify that exact final history before
requesting canonical transfer.
The receipt is local to this database. The manager must bind its authenticated
runtime/instance and checkout; detached JSON from another runtime or database is
not drain proof for the original writer.

The fence and work tickets use the existing native SQLite/KV transaction path,
not a second database or an agent label. Execution, registered tools, owned shell
process scopes and session-owned jobs record admission before dispatch. Successful
work removes its ticket only after its owned scope settles. Failures, interruption
or process death retain unresolved tickets. No timer, PID probe, idle observation,
interrupt acknowledgement or connection close clears them. This conservative first
slice may require recovery even after a harmless tool failure; it has no automatic
recovery, fence release or readmission operation.

Root fences also reject descendant admission, retained tool snapshots and fresh
execution owners after database reopening. Existing input is not silently dropped;
queued work blocks certification. A family above 256 sessions is refused rather
than partially certified. An old background marker without the v1 ownership
contract remains uncertified even if its status says completed.

This proves cessation of runtime-owned dispatch/work, not rollback of external
effects or supervision of arbitrary detached processes launched outside the native
tool/job contract. Interrupted or uncertain tool operations cannot yield a receipt.
Before acknowledging either phase, the API promotes and verifies the SQLite
connection's synchronous setting to EXTRA, keeping it there (including when WAL
is unavailable and SQLite uses a rollback journal).
Backends without this durability primitive are refused. Tests establish the
setting and process-restart persistence, not physical power-loss acceptance;
storage and restored-backup reconciliation remain separate activation gates.
A mixed database with unfenced old binaries is unsupported:
drain incompatible writers before enabling the capability, and do not roll back
to such a binary after writing fences. Reconcile legacy work before activation;
this does not retrospectively certify work whose old markers were already lost.

## Integration gates

Before exposure: independently review the native boundary; version the native
HTTP/client capability; prove the manager's durable source block and all queued
prompt/permission paths; bind the native receipt to the exact canonical source,
writer generation and final synchronized pin; implement authorized recovery and
readmission. Canonical publication, adapted-history verification, recipient
revocation and the backend's global migration drain remain separate requirements.
The native receipt alone grants neither import nor model execution elsewhere.

The deterministic cases live in `session-execution.test.ts`, `job.test.ts`,
`tool-registry.test.ts` and `session-shell.test.ts`. They use real native database,
coordinator, tool registry and subprocess-backed shell boundaries, with controlled
runner effects instead of model calls. Run them through Core's existing isolated
`bun run test` wrapper. The root `bun run check` retains every lint/type-check task
with at most two concurrent task workers, including its pre-commit invocation.
