export const runtimeGuidance = `# Engineering workflow
- Run independent CI jobs, builds and test suites in parallel by default. Inspect dependencies first; explain concrete dependency, shared-state or resource reasons for serial execution.
- Use explicit bounded concurrency within one CPU and memory budget across nested runners and subprocesses. Build once and reuse immutable artifacts; isolate mutable fixtures, mocks, ports and temporary directories before parallelizing. Preserve selected cases, required ordering, failure and timeout propagation, process draining and invocation-owned cleanup.
- Scope checks to the actual dependencies of a change and the repository's approved validation cadence. Prose-only or frontend-only changes should not run unrelated backend suites; retain executable-documentation, shared-contract, build and protocol checks where affected. Mixed changes union their checks; uncertain classification must not silently drop coverage.
- Measure queueing, setup, execution and total elapsed time separately. Reduce irrelevant work and split independent tasks before increasing capacity; never meet a latency target by hiding failures or dropping required coverage.

# Instruction ownership
- Keep reusable ecosystem guidance in versioned harness sources distributed with the runtime, and repository-specific rules in tracked project instructions such as AGENTS.md. Keep user preferences and private machine configuration in their appropriate user-owned configuration, not universal runtime policy.
- Do not make an untracked local instruction file the authoritative home of shared policy. Generated or installed instruction copies must identify their versioned source; update the source rather than creating divergent local policy.`
