# Session environment recovery carry

Source-only candidate based on `carry/2.0.15-chi.1`. This does not change the
installed binary or admit another version through Chi's transfer gate.

## Failure boundary

`session.environment` replaces the local shell environment for one native session.
Its values live in a process-local map, not the session database or a directory
instance. A successor server previously resumed durable executions with an empty
map and fell back to its own process environment. A service launched by agent A
could therefore run recovered agent B's shells with A's identity.

The live-process claim guard from upstream PR #51215 protects a living owner; it
does not restore an environment after that owner exits. An independent service
does not necessarily load the original client's plugins, so a Paseo bridge-only
repair cannot protect this boundary.

## Candidate contract

Binding an environment persists only a boolean `session.environment/<sessionID>`
requirement in private KV storage. Environment values remain in memory. Previously
bound local shells wait for their own environment to be reattached; unrelated
sessions remain usable. Root recovery waits before claiming execution or consuming
the restart budget, coalesces repeated sweeps, and rechecks claim liveness and
ownership after reattachment. Session deletion clears the marker after deletion.

Paseo separately reapplies its full environment snapshot, with the correct agent's
identity layered on top, on each event-stream connection. An unattached session
may remain waiting until its owning client supplies an environment.

## Validation and rollout

The focused core environment, execution, deletion and real shell suites exercise
two sessions in one directory, independent reattachment and repeated recovery
sweeps. Restoring the old shell fallback makes the regression print the test
process's agent identity instead of the session identity.

Before rollout, build a separately versioned candidate, repeat the Chi transfer
and mixed-version gate, then deploy both hosts in a coordinated change. Do not
reuse the installed version label. Start services without caller-agent identity
variables and reattach managed sessions before admitting work: sessions created
by older binaries have no durable requirement until their first binding under
the candidate. No historical environment can be reconstructed from the database.
Do not backfill environment values or credentials into session history.

Upstream issue: https://github.com/anomalyco/opencode/issues/51784.
