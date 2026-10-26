# Optional automatic-compaction input cap

`compaction.input_cap` is an optional positive integer. It caps the existing
estimated-input trigger, not the advertised model capacity or output limit.
There is no new default. A user's global configuration can opt in:

```json
{
  "compaction": {
    "auto": true,
    "input_cap": 128000,
    "keep": { "tokens": 15000 }
  }
}
```

Keep this preference in user-owned configuration, not model definitions or
universal runtime guidance. Existing project/config precedence still applies;
a project override or `auto: false` can change the effective policy.

The trigger is the minimum of the cap and the existing input/context/output
safety ceilings. A smaller model therefore still compacts earlier. For unknown
context capacity, an explicit cap and any known input ceiling apply without
inventing a model capacity; absent a cap, previous behavior is unchanged.
The generic Location-scoped service is shared by main, named and linked-child
sessions and uses the currently resolved model, including custom routes.
Standalone Codex and Claude runtimes do not inherit this OpenCode setting.

This is **not a hard 128,000-token request guarantee**. Usage-based estimation
includes the previous response because that response becomes next-request input.
Active calls and local tools settle first, and pending hosted tools retain their
continuation owner. A just-completed checkpoint runs before another automatic
compaction; native checkpoints need primary-response usage to anchor their new
window. These guards avoid immediate ineffective-compaction loops. A large
checkpoint, tool result or indivisible newest exchange can exceed the trigger.
The retained-tail allowance is also approximate: whole exchanges stay together.

## Lessons audited from Chi's JSONL reducer

Reference: canonical `chi/packages/jsonl-reduce`, inspected at source commit
`360db89a5eb44217a5bb3f3b952fa8766f28c234`. This is a source-contract and test
inspection, not a claim that the Chi tests were executed in this worktree.
No Pi runtime, dependency or second state store is introduced.

| Reducer mechanism and evidence                                                                                                                                      | OpenCode application and boundary                                                                                                                                                                                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/types.ts` separates `ReductionMetadata` from semantic text; `reduce.test.ts` checks exact lineage and deterministic field algebra.                             | Preserve existing durable messages, IDs, usage events, model references and provider checkpoint provenance outside summary text. The cap changes only the trigger; summaries are not an authority for accounting, identity, approvals or completed work.                                                       |
| `src/mentions.ts` retains unknown actors as null and rejects conflicting duplicates; `mentions.test.ts` checks both.                                                | Do not infer missing actor identities or cleared holds from compressed prose. Existing typed records remain authoritative. This patch does not add an equivalent mention index or claim one already exists.                                                                                                    |
| `src/selection.ts` validates causal order, rejects overlapping coverage and checks artifact hashes; `selection.test.ts` exercises those failures.                   | Preserve ordered Session history and tool-owner settlement before replacing the request window. Runner regressions cover local-tool and hosted-tool boundaries. OpenCode checkpoints are not JSONL selection artifacts: they do not acquire a raw-entry coverage hash or transitive lineage merely by analogy. |
| `reduce.test.ts` excludes projected titles from semantic hashes/coverage and tests transitive lineage; `selection.test.ts` tests reducing a previous immutable output with uncovered new entries. | Do not feed generated labels back as independent evidence. Prior summaries can remain semantic context, but cannot manufacture new evidence or exact source coverage. The audited reducer permits controlled summary reuse; it does not support a blanket claim that summaries never enter later reductions. |
| Deterministic metadata and immutable source coordinates survive model compression.                                                                                  | Original Session records and instruction state remain separate from the model-written checkpoint. The cap neither deletes history nor forks/resets the Session or expires a hold. Exact claim-level evidence retention inside free-form summaries is not guaranteed by this change.                            |

## Validation and activation boundaries

Focused config/compaction/runner tests cover the exact cap boundary, smaller and
unknown-capacity models, native and summary modes, custom routes, named agents,
child contexts, model switches, config reload/removal, inherited tail settings,
tool settlement and immediate-loop guards. Generated clients must be refreshed
from the schema, not edited by hand.

Source checks do not certify a binary or activate the policy. Installation still
requires the reviewed carry head, required CI, immutable artifact provenance and
the owned installation/rollback path. Apply the user preference only when the
installed schema supports it; preserve unrelated configuration. Live acceptance
must establish the effective policy separately from source-test results.
