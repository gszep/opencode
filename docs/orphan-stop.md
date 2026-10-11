# Cancelling an original orphan without replay

A root execution can retain its write-ahead claim after its process dies.
An empty process-local active set does not make that intent idle: a managed
service restart selects the claim, queues a continuation notice and resumes it.

The existing interrupt endpoint now also handles an explicit Stop of an original,
unresumed root claim whose owner is proven absent. It publishes the normal user
interruption terminal and releases that exact claim in the same transaction.
It does not run the model, deliver inbox entries, clear queues, cancel unrelated
background work or remove history. The normal terminal projection appends an idle
marker and clears any current retry display state. Existing content remains.

No new endpoint, request option or store is introduced. `resume=true` retains its
existing steering semantics and must not be used for orphan cancellation. Idle
sessions, live owners, unowned children and claims with a recovery attempt remain
no-ops. The exact PID/claim time/attempts are checked again at commit; failed
process probes and live PID reuse fail closed. Historical claims record a PID,
not a process-start lease: do not infer that a currently live PID is disposable.

The root resume counter now requires a remaining claim. This closes the race in
which recovery selected a root before Stop but tried to resume it afterwards.
Children retain their separate background-job recovery path.

## Source candidate and controlled use

Use the distinct private candidate version `2.0.15-chi.2-orphan-stop.1` when
building with the existing CLI build script. Do not overwrite or relabel the
sealed `2.0.15-chi.2` binaries. This candidate is not a new admitted Chi writer.
Source review, synthetic tests and the compiled isolated API check must pass
before any operational use.

The existing CLI `--standalone` option starts a stdio-leased server without the
managed lifecycle hook, so it does not invoke startup recovery. That permits a
reviewed private candidate to call the existing interrupt endpoint before a
managed service replacement, without installing it as the live runtime:

```sh
/absolute/reviewed/candidate api post /api/session/EXACT_SESSION_ID/interrupt --standalone
```

For operational reconciliation, first record the exact intended session IDs and
claim metadata, verify the relevant owners are gone, and coordinate native and
managed idle. Use normal runtime calls only, never direct database edits. Verify
each return value, the terminal/claim and untouched queued entries. Do not use
`resume`, replay old notices, or restart a managed service merely to make the Stop
endpoint available. A false response is not permission to clear state manually.

Dead shell-job recovery is distinct: it admits a cancellation notification with
`resume:false`, then removes its scheduler record. It does not rerun the shell
command or wake an otherwise idle owner. Valid unfinished subagent recovery and
top-level claims can run work; classify them separately rather than treating all
running-labelled records as either harmless or active.
