# Carried system-update/tool-result fix

Source-only integration candidate, September 28, 2026. Based on linkfix branch
tip `aa665f75d51952f09fb0ea211a711de623d25caa`; no executable was built, installed,
or restarted for this change.

## Two distinct failure paths

1. Two local servers sharing a database can both drive an execution when the
   managed service recovers another live server's claim. Carry the existing
   upstream proposal [#51215](https://github.com/anomalyco/opencode/pull/51215),
   commits `f9b01d2d1b963878bbcc4b03e974c51d40cf2dc9` and
   `8df933c38fba8ccbd1b87cbd6062871cf4de2959`, rather than reimplementing it.
   The cherry-picks retain their upstream attribution. This proposal is unmerged;
   its nullable claim PID migration and background-job PID metadata are part of
   this candidate. Existing claims without PID retain the old recovery behavior.
   It is a same-host recovery guard, not distributed execution fencing.
2. Anthropic lowering rejects a text update between a call and an available
   result, including an error result. The shared history normalizer can also
   insert a missing-result error *after* an update when a later user/assistant
   message exists. Defer text updates until all outstanding local results have
   been emitted, preserving update order. Use that normalized sequence for native
   versus wrapped placement. Do not mutate the input, cross another conversation
   message, or manufacture results in the protocol adapter. Terminal missing
   results remain invalid. Issue [#51764](https://github.com/anomalyco/opencode/issues/51764)
   and focused upstream PR [#51765](https://github.com/anomalyco/opencode/pull/51765)
   target `v2` at `0caae608a28819981510989d28768cd9d4e4a663`.

The first defect explains overlapping executions; the second prevents already
recoverable history from failing request compilation. The lowering fix alone
cannot make a concurrently pending terminal call safe to replay.

## Environment updates

`InstructionBuiltIns` hashes the rendered `core/environment` source, including
`Global.tmp`. `Global` resolves the actual temporary directory through `realpath`;
the path is also used for approved file access. The implementation contains no
same-host volatile-field exclusion. Different real temp roots are meaningful
runtime differences, so this change does not suppress their updates. Aligning
server environments reduces update churn but does not replace execution ownership.

## Deterministic verification

Use the user-local Bun 1.4.2 and Node 24.18.0 toolchain from
[`linkfix-build.md`](./linkfix-build.md). Run from the relevant package:

```sh
# packages/ai
bun test test/provider/anthropic-messages.test.ts test/provider/google-vertex.test.ts test/effort-updates.test.ts test/tool-history.test.ts --only-failures
bun typecheck
# packages/core
bun test test/session-execution.test.ts --timeout 30000 --only-failures
bun test test/session-runner-message.test.ts test/v1-migration.test.ts --timeout 30000 --only-failures
bun typecheck
bun run migration --check
# repository root
bun run check
```

The lowering regression was observed failing before the adapter change. Cases
cover parallel results, completed/cancelled/interrupted results, ordered multiple
updates, terminal updates, shared missing-result repair, unchanged input, direct
Anthropic Opus 4.8/5.5, older-model fallback, Vertex fallback, and rejection of
missing results or movement across another conversation message. No live model
or private session payload is part of the regression fixtures.

The carried base passes 121 focused AI tests, 46 execution tests, 51 message and
migration tests, migration schema validation, and the 35-task root check. The
upstream candidate passes 132 focused AI tests and the 35-task root check.

## Combined-build integration

The delta from `aa665f75d` applies cleanly with `git apply --cached --3way` to
`feat/plugin-prompt-completions` at `508492110` (verified in a disposable index;
that branch was not modified). A full branch merge still inherits the original
linkfix base's pre-existing `packages/tui/src/ui/link.tsx` conflict against newer
upstream: preserve the modified-click guard and use upstream's `openUrl` import
and call. This is the already documented linkfix adaptation, not a conflict in
the system-update/recovery patch. The orchestrator owns combined-build integration.
