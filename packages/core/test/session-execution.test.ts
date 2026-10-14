import { describe, expect, test, spyOn } from "bun:test"
import { AIError, TransportError } from "@opencode/ai"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Bus } from "@opencode/core/bus"
import { Instance } from "@opencode/core/instance/service"
import { Job } from "@opencode/core/job"
import { KV } from "@opencode/core/kv"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import type { LocationServices } from "@opencode/core/location-services"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionDrain } from "@opencode/core/session/drain"
import { EventSequenceTable } from "@opencode/core/event/sql"
import { SessionRestart } from "@opencode/core/session/execution/restart"
import { UserInterruptedError } from "@opencode/core/session/error"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionRunner } from "@opencode/core/session/runner/index"
import { SessionInboxTable, SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, LayerMap, Schedule, Scope } from "effect"
import { eq, sql } from "drizzle-orm"
import { join } from "node:path"
import { Global } from "@opencode/util/global"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, Bus.node, SessionStore.node, SessionInbox.node, Job.node, KV.node, Session.node]),
  ),
)

describe("SessionExecution lifecycle", () => {
  it.live("drain review: real processes share admission fencing and hard-killed work stays unresolved", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      const filename = join(tmp.path, "process-fence.sqlite")
      const script = join(tmp.path, "drain-worker.mjs")
      yield* Effect.promise(() =>
        Bun.write(
          script,
          `
        import { Effect, Exit } from ${JSON.stringify(import.meta.resolve("effect"))};
        import { Database } from ${JSON.stringify(import.meta.resolve("@opencode/core/database/database"))};
        import { SessionDrain } from ${JSON.stringify(import.meta.resolve("@opencode/core/session/drain"))};
        import { Session } from ${JSON.stringify(import.meta.resolve("@opencode/schema/session"))};
        import { Global } from ${JSON.stringify(import.meta.resolve("@opencode/util/global"))};
        const [filename, id, mode, marker] = process.argv.slice(2);
        await Effect.runPromise(Effect.gen(function* () {
          const db = (yield* Database.Service).db;
          const admission = yield* SessionDrain.enter(db, Session.ID.make(id), 'tool').pipe(Effect.exit);
          if (Exit.isFailure(admission)) {
            yield* Effect.promise(() => Bun.write(marker + '.denied', 'denied'));
            return;
          }
          yield* Effect.promise(() => Bun.write(marker + '.executed', 'owned work'));
          if (mode === 'hold') {
            const deadline = Date.now() + 10000;
            while (!(yield* Effect.promise(() => Bun.file(marker + '.release').exists()))) {
              if (Date.now() > deadline) return yield* Effect.die('fixture deadline');
              yield* Effect.promise(() => Bun.sleep(10));
            }
          }
          yield* admission.value.finish(Exit.void);
        }).pipe(Effect.provide(Database.layer({ path: filename })),
          Effect.provideService(Global.Service, Global.make({ data: ${JSON.stringify(tmp.path)} })), Effect.scoped));
      `,
        ),
      )
      const worker = (id: Session.ID, mode: string, marker: string) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            const child = Bun.spawn({
              cmd: [process.execPath, script, filename, id, mode, marker],
              cwd: tmp.path,
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
            })
            const output = Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
            return { child, output }
          }),
          ({ child, output }) =>
            Effect.promise(async () => {
              if (child.exitCode === null) child.kill("SIGKILL")
              await child.exited
              await output
            }).pipe(Effect.orDie),
        )
      const waitFile = (name: string) =>
        Effect.promise(() => Bun.file(name).exists()).pipe(
          Effect.repeat({ until: (exists) => exists, schedule: Schedule.spaced("10 millis") }),
          Effect.timeout("5 seconds"),
        )
      const layer = Database.layer({ path: filename }).pipe(
        Layer.provide(Layer.succeed(Global.Service, Global.make({ data: tmp.path }))),
      )
      yield* Effect.gen(function* () {
        const database = yield* Database.Service
        const root = Session.ID.make("ses_process_fence")
        const lost = Session.ID.make("ses_process_lost")
        yield* seedSessions(database, [root, lost])
        const marker = join(tmp.path, "first")
        const first = yield* worker(root, "hold", marker)
        yield* waitFile(marker + ".executed")
        const request = {
          v: 1 as const,
          sessionID: root,
          requestID: "00000000-0000-4000-8000-000000000094",
          generation: 0,
          sequence: -1,
        }
        yield* SessionDrain.begin(request)
        expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
        yield* Effect.promise(() => Bun.write(marker + ".release", ""))
        expect(yield* Effect.promise(() => first.child.exited)).toBe(0)
        expect((yield* SessionDrain.complete(request)).phase).toBe("quiescent")
        const retryMarker = join(tmp.path, "retry")
        const retry = yield* worker(root, "try", retryMarker)
        expect(yield* Effect.promise(() => retry.child.exited)).toBe(0)
        expect(yield* Effect.promise(() => Bun.file(retryMarker + ".denied").exists())).toBe(true)
        expect(yield* Effect.promise(() => Bun.file(retryMarker + ".executed").exists())).toBe(false)

        const lostMarker = join(tmp.path, "lost")
        const interrupted = yield* worker(lost, "hold", lostMarker)
        yield* waitFile(lostMarker + ".executed")
        const unresolved = { ...request, sessionID: lost, requestID: "00000000-0000-4000-8000-000000000095" }
        yield* SessionDrain.begin(unresolved)
        yield* Effect.sync(() => interrupted.child.kill("SIGKILL"))
        expect(yield* Effect.promise(() => interrupted.child.exited)).not.toBe(0)
        expect(yield* SessionDrain.complete(unresolved).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      }).pipe(Effect.provide(Layer.fresh(layer)), Effect.scoped)
    }),
  )

  it.effect("drain review: settlement capability cannot admit a different input or survive its ticket", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const root = Session.ID.make("ses_settlement_capability")
      yield* seedSessions(database, [root])
      const ticket = yield* SessionDrain.enter(database.db, root, "session-shell")
      const wrong = yield* SessionDrain.enter(database.db, root, "tool")
      const inboxID = SessionMessage.ID.make("msg_owned_completion")
      const permit = { shellID: "sh_owned_completion", inboxID }
      const data = {
        sessionID: root,
        inboxID,
        item: {
          type: "synthetic",
          delivery: "steer",
          payload: { text: "completion", metadata: { source: "shell", shellID: permit.shellID } },
        },
      } as const
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000093",
        generation: 0,
        sequence: -1,
      }
      yield* SessionDrain.begin(request)
      expect(yield* bus.publish(SessionEvent.InboxEnqueued, data).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect(
        yield* wrong.settle(permit, bus.publish(SessionEvent.InboxEnqueued, data)).pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      expect(
        yield* ticket
          .settle(
            permit,
            bus.publish(SessionEvent.InboxEnqueued, {
              ...data,
              inboxID: SessionMessage.ID.make("msg_different_completion"),
            }),
          )
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      expect(
        yield* ticket
          .settle(
            permit,
            bus.publish(SessionEvent.InboxEnqueued, {
              ...data,
              item: { type: "user", delivery: "queue", payload: { text: "new work" } },
            }),
          )
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      yield* wrong.finish(Exit.void)
      yield* ticket.finish(Exit.void)
      expect((yield* SessionDrain.complete(request)).phase).toBe("quiescent")
      expect(
        yield* ticket.settle(permit, bus.publish(SessionEvent.InboxEnqueued, data)).pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      expect((yield* SessionDrain.complete(request)).sequence).toBe(-1)
    }),
  )

  it.effect("drain v1 serializes work admission with fence publication in both orders", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const first = Session.ID.make("ses_fence_admission_first")
      const second = Session.ID.make("ses_fence_publish_first")
      yield* seedSessions(database, [first, second])
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const admission = yield* database.db
        .transaction(() =>
          Effect.gen(function* () {
            const ticket = yield* SessionDrain.enter(database.db, first, "tool")
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
            return ticket
          }),
        )
        .pipe(Effect.orDie, Effect.forkScoped)
      yield* Deferred.await(entered)
      const request = {
        v: 1 as const,
        sessionID: first,
        requestID: "00000000-0000-4000-8000-000000000040",
        generation: 0,
        sequence: -1,
      }
      const pending = yield* SessionDrain.begin(request).pipe(Effect.forkScoped)
      yield* Deferred.succeed(release, undefined)
      const ticket = yield* Fiber.join(admission)
      yield* Fiber.join(pending)
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      yield* ticket.finish(Exit.void)
      expect((yield* SessionDrain.complete(request)).phase).toBe("quiescent")

      const frozen = yield* Deferred.make<void>()
      const publish = yield* Deferred.make<void>()
      const next = { ...request, sessionID: second, requestID: "00000000-0000-4000-8000-000000000041" }
      const fence = yield* database.db
        .transaction(() =>
          Effect.gen(function* () {
            yield* SessionDrain.begin(next)
            yield* Deferred.succeed(frozen, undefined)
            yield* Deferred.await(publish)
          }),
        )
        .pipe(Effect.orDie, Effect.forkScoped)
      yield* Deferred.await(frozen)
      const late = yield* SessionDrain.enter(database.db, second, "tool").pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.succeed(publish, undefined)
      yield* Fiber.join(fence)
      expect(yield* Fiber.join(late)).toMatchObject({ _tag: "Failure" })
      expect((yield* SessionDrain.complete(next)).phase).toBe("quiescent")
    }),
  )

  it.effect("drain v1 refuses legacy background work even without a live execution claim", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const kv = yield* KV.Service
      const root = Session.ID.make("ses_fence_legacy_job")
      yield* seedSessions(database, [root])
      yield* kv.set("job.background/msg_unacknowledged", {
        id: "job_unacknowledged",
        notificationID: "msg_unacknowledged",
        recovery: { kind: "shell", sessionID: root, shellID: "shell_unacknowledged", command: "synthetic" },
        status: "running",
        pid: 1,
      })
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000009",
        generation: 0,
        sequence: -1,
      }
      yield* SessionDrain.begin(request)
      expect((yield* claims(database))[root]).toBe(false)
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      yield* kv.set("job.background/msg_unacknowledged", {
        id: "job_unacknowledged",
        notificationID: "msg_unacknowledged",
        recovery: { kind: "shell", sessionID: root, shellID: "shell_unacknowledged", command: "synthetic" },
        status: "completed",
        pid: 1,
      })
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.live("drain v1 persists proof and unresolved work across closing and reopening SQLite", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped()
      const layer = Database.layer({ path: join(tmp.path, "fence.sqlite") }).pipe(
        Layer.provide(Layer.succeed(Global.Service, Global.make({ data: tmp.path }))),
      )
      const root = Session.ID.make("ses_fence_disk")
      const orphan = Session.ID.make("ses_fence_disk_orphan")
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000007",
        generation: 0,
        sequence: -1,
      }
      const unknown = { ...request, sessionID: orphan, requestID: "00000000-0000-4000-8000-000000000008" }
      yield* Effect.gen(function* () {
        const database = yield* Database.Service
        yield* seedSessions(database, [root, orphan])
        // Certify in rollback-journal mode too; reopening uses the normal WAL bootstrap.
        yield* database.db.run(sql`PRAGMA journal_mode = DELETE`).pipe(Effect.orDie)
        expect(
          (yield* database.db.get<{ journal_mode: string }>(sql`PRAGMA journal_mode`).pipe(Effect.orDie))?.journal_mode,
        ).toBe("delete")
        yield* SessionDrain.begin(request)
        expect((yield* SessionDrain.complete(request)).phase).toBe("quiescent")
        expect(
          (yield* database.db.get<{ synchronous: number }>(sql`PRAGMA synchronous`).pipe(Effect.orDie))?.synchronous,
        ).toBe(3)
        // Simulates a process lost after dispatch: no completion acknowledgement.
        yield* SessionDrain.enter(database.db, orphan, "tool")
        yield* SessionDrain.begin(unknown)
      }).pipe(Effect.provide(Layer.fresh(layer)), Effect.scoped)
      yield* Effect.gen(function* () {
        const database = yield* Database.Service
        expect((yield* SessionDrain.complete(request)).phase).toBe("quiescent")
        expect(
          (yield* database.db.get<{ synchronous: number }>(sql`PRAGMA synchronous`).pipe(Effect.orDie))?.synchronous,
        ).toBe(3)
        expect(yield* SessionDrain.enter(database.db, root, "execution").pipe(Effect.exit)).toMatchObject({
          _tag: "Failure",
        })
        expect(yield* SessionDrain.complete(unknown).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
        expect((yield* SessionDrain.begin(unknown)).phase).toBe("holding")
      }).pipe(Effect.provide(Layer.fresh(layer)), Effect.scoped)
    }),
  )

  it.effect("drain v1 preserves queued input and rejects new admission or child creation", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const inbox = yield* SessionInbox.Service
      const root = Session.ID.make("ses_fence_queued")
      yield* seedSessions(database, [root])
      yield* seedInbox(database, root, ["queue"])
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000005",
        generation: 0,
        sequence: -1,
      }
      yield* SessionDrain.begin(request)
      expect(
        yield* inbox
          .admit({
            id: SessionMessage.ID.make("msg_denied"),
            sessionID: root,
            item: { type: "user", delivery: "queue", payload: { text: "must not run" } },
          })
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      expect(
        yield* bus
          .publish(SessionEvent.Created, {
            sessionID: Session.ID.make("ses_fence_new_child"),
            parentID: root,
            projectID: Project.ID.global,
            location: { directory: AbsolutePath.make("/project") },
            slug: "child",
            version: "test",
          })
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      expect(
        yield* database.db
          .select()
          .from(SessionInboxTable)
          .where(eq(SessionInboxTable.session_id, root))
          .all()
          .pipe(Effect.orDie),
      ).toHaveLength(1)
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.effect("drain v1 does not infer cessation from an interrupted tool ticket", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const root = Session.ID.make("ses_fence_uncertain")
      yield* seedSessions(database, [root])
      const entered = yield* Deferred.make<void>()
      const running = yield* SessionDrain.work(
        database.db,
        root,
        "tool",
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      ).pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000006",
        generation: 0,
        sequence: -1,
      }
      yield* SessionDrain.begin(request)
      yield* Fiber.interrupt(running)
      expect((yield* claims(database))[root]).toBe(false)
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.effect("drain v1 fences new execution and children, then certifies only settled owned work", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const root = Session.ID.make("ses_fence_root")
      const child = Session.ID.make("ses_fence_child")
      yield* seedSessions(database, [root])
      yield* seedSessions(database, [child], { parent_id: root })
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      )
      const execution = Context.get(context, SessionExecution.Service)
      const running = yield* execution.resume(child).pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000001",
        generation: 0,
        sequence: -1,
      }
      expect((yield* SessionDrain.begin(request)).phase).toBe("holding")
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect(yield* execution.resume(root).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect((yield* claims(database))[child]).toBe(true)
      const contenderScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(contenderScope, Exit.void))
      const contender = yield* buildExecution(contenderScope, () => Effect.die("fenced contender dispatched"))
      expect(yield* Context.get(contender, SessionExecution.Service).resume(child).pipe(Effect.exit)).toMatchObject({
        _tag: "Failure",
      })
      expect((yield* claims(database))[child]).toBe(true)
      // Rejected starts cannot release the already running child's claim.
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(running)
      yield* execution.awaitIdle(child)
      const proof = yield* SessionDrain.complete(request)
      expect(proof).toEqual({ request, generation: 1, phase: "quiescent", sequence: -1 })
      expect(yield* SessionDrain.begin(request)).toEqual(proof)
      expect(yield* SessionDrain.complete(request)).toEqual(proof)
      expect(yield* execution.resume(child).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect(yield* execution.active).toEqual(new Set())
      yield* database.db.delete(SessionTable).where(eq(SessionTable.id, root)).run().pipe(Effect.orDie)
      expect(yield* SessionDrain.assertOpen(database.db, child).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.effect("drain v1 binds the observed native sequence and never retargets a request", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const root = Session.ID.make("ses_fence_sequence")
      yield* seedSessions(database, [root])
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      )
      const execution = Context.get(context, SessionExecution.Service)
      const running = yield* execution.resume(root).pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      const head = yield* database.db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, root))
        .get()
        .pipe(Effect.orDie)
      if (!head) return yield* Effect.die("missing native sequence")
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000002",
        generation: 0,
        sequence: head.seq,
      }
      expect(yield* SessionDrain.begin({ ...request, sequence: -1 }).pipe(Effect.exit)).toMatchObject({
        _tag: "Failure",
      })
      const results = yield* Effect.all(
        [request, { ...request, requestID: "00000000-0000-4000-8000-000000000003" }].map((input) =>
          SessionDrain.begin(input).pipe(Effect.exit),
        ),
        { concurrency: 2 },
      )
      expect(results.map((result) => result._tag).sort()).toEqual(["Failure", "Success"])
      const success = results.find(Exit.isSuccess)
      if (!success) return yield* Effect.die("missing winning fence")
      const selected = success.value.request
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(running)
      const proof = yield* SessionDrain.complete(selected)
      expect(proof.request).toEqual(selected)
      expect(proof.sequence).toBeGreaterThan(selected.sequence)
      expect(proof.generation).toBe(1)
      expect(yield* SessionDrain.complete({ ...selected, generation: 1 }).pipe(Effect.exit)).toMatchObject({
        _tag: "Failure",
      })
    }),
  )

  it.effect("drain v1 survives a fresh execution/recovery scope; shutdown and old claims are not proof", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const root = Session.ID.make("ses_fence_restart")
      yield* seedSessions(database, [root])
      const entered = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      const original = yield* buildExecution(scope, () =>
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      )
      yield* Context.get(original, SessionExecution.Service).resume(root).pipe(Effect.forkIn(scope))
      yield* Deferred.await(entered)
      const head = yield* database.db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, root))
        .get()
        .pipe(Effect.orDie)
      if (!head) return yield* Effect.die("missing native sequence")
      const request = {
        v: 1 as const,
        sessionID: root,
        requestID: "00000000-0000-4000-8000-000000000004",
        generation: 0,
        sequence: head.seq,
      }
      yield* SessionDrain.begin(request)
      yield* Scope.close(scope, Exit.void)
      expect((yield* claims(database))[root]).toBe(true)
      const next = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(next, Exit.void))
      let runs = 0
      const restarted = yield* buildExecution(next, () =>
        Effect.sync(() => {
          runs++
        }),
      )
      yield* Context.get(restarted, SessionRestart.Service).resumeSuspendedSessions
      expect(runs).toBe(0)
      expect(yield* Context.get(restarted, SessionExecution.Service).resume(root).pipe(Effect.exit)).toMatchObject({
        _tag: "Failure",
      })
      expect(runs).toBe(0)
      expect((yield* claims(database))[root]).toBe(true)
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect((yield* SessionDrain.begin(request)).phase).toBe("holding")
    }),
  )
  test("classifies success and typed failure terminals", () => {
    expect(SessionExecution.terminal(Exit.succeed(undefined))).toEqual({ type: "succeeded" })
    expect(
      SessionExecution.terminal(
        Exit.fail(
          new AIError({
            reason: new TransportError({ message: "Disconnected", transport: "http", operation: "request" }),
          }),
        ),
      ),
    ).toEqual({ type: "failed", error: { type: "provider.transport", message: "Disconnected" } })
  })

  test("defaults owner-scope interruption to shutdown and preserves explicit reasons", () => {
    const interrupted = Effect.runSyncExit(Effect.interrupt)
    expect(SessionExecution.terminal(interrupted)).toEqual({ type: "interrupted", reason: "shutdown" })
    expect(SessionExecution.terminal(interrupted, "user")).toEqual({ type: "interrupted", reason: "user" })
    expect(SessionExecution.terminal(Exit.fail(new UserInterruptedError()))).toEqual({
      type: "interrupted",
      reason: "user",
    })
  })

  it.effect("the sweep only lists claimed top-level Sessions", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const parent = Session.ID.make("ses_recover_parent")
      const child = Session.ID.make("ses_recover_child")
      const idle = Session.ID.make("ses_recover_idle")
      yield* seedSessions(database, [parent], { time_suspended: Date.now() })
      yield* seedSessions(database, [idle])
      // Children recover through background Job records, never through the root claim sweep.
      yield* seedSessions(database, [child], { time_suspended: Date.now(), parent_id: parent })

      expect(yield* store.listSuspended()).toEqual([parent])

      // The sweep clears orphaned child claims outright; parents keep theirs.
      yield* store.releaseChildClaims([])
      expect(yield* claims(database)).toEqual({ [parent]: true, [child]: false, [idle]: false })
    }),
  )

  it.effect("claims at execution start, releases on completion, and preserves through teardown", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const interrupted = Session.ID.make("ses_claim_interrupted")
      const completed = Session.ID.make("ses_claim_completed")
      yield* seedSessions(database, [interrupted, completed])

      // Each drain signals once it runs; the claim commits before the drain starts.
      const interruptedRunning = yield* Deferred.make<void>()
      const completedRunning = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID }) =>
        sessionID === completed
          ? Deferred.succeed(completedRunning, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Deferred.succeed(interruptedRunning, undefined).pipe(Effect.andThen(Effect.never)),
      )
      const execution = Context.get(context, SessionExecution.Service)
      const completedActive = execution.isActive(completed)
      expect(yield* completedActive).toBe(false)
      yield* execution.resume(interrupted).pipe(Effect.forkScoped)
      const completing = yield* execution.resume(completed).pipe(Effect.forkIn(scope))
      yield* Deferred.await(interruptedRunning)
      yield* Deferred.await(completedRunning)

      // The write-ahead claim exists WHILE the turns run — no shutdown hook involved.
      expect(yield* claims(database)).toEqual({ [interrupted]: true, [completed]: true })
      expect(yield* completedActive).toBe(true)
      expect(yield* execution.isActive(interrupted)).toBe(true)

      // A drain that finishes on its own releases its claim.
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(completing)
      yield* execution.awaitIdle(completed)
      expect((yield* claims(database))[completed]).toBe(false)
      expect(yield* completedActive).toBe(false)
      expect(yield* execution.isActive(interrupted)).toBe(true)

      // Teardown interruption (graceful twin of an unclean death) preserves the claim
      // for the next server start.
      yield* Scope.close(scope, Exit.void)
      expect((yield* claims(database))[interrupted]).toBe(true)
      expect(yield* execution.isActive(interrupted)).toBe(false)
    }),
  )

  it.effect("a user interrupt releases the claim so the turn never resurrects", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_claim_user_cancel")
      yield* seedSessions(database, [sessionID])

      const draining = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never)),
      )
      const execution = Context.get(context, SessionExecution.Service)
      yield* execution.resume(sessionID).pipe(Effect.forkScoped)
      yield* Deferred.await(draining)
      expect((yield* claims(database))[sessionID]).toBe(true)

      expect(yield* execution.interrupt(sessionID)).toBeTrue()
      yield* execution.awaitIdle(sessionID)
      expect((yield* claims(database))[sessionID]).toBe(false)
    }),
  )

  it.effect("reports an idle interrupt as a no-op", () =>
    Effect.gen(function* () {
      const sessionID = Session.ID.make("ses_idle_cancel")
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () => Effect.never)
      const execution = Context.get(context, SessionExecution.Service)

      expect(yield* execution.interrupt(sessionID)).toBeFalse()
      expect(yield* execution.active).not.toContain(sessionID)
      expect(yield* execution.isActive(sessionID)).toBe(false)
    }),
  )

  it.effect("stops a dead owner's original root intent without draining or advancing its inbox", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const sessionID = Session.ID.make("ses_original_orphan_stop")
      const dead = yield* exitedProcess
      yield* seedSessions(database, [sessionID], { claim_pid: dead, time_suspended: 123 })
      const priorID = SessionMessage.ID.make("msg_before_orphan_stop")
      yield* database.db
        .insert(SessionMessageTable)
        .values({
          id: priorID,
          session_id: sessionID,
          type: "user",
          seq: 0,
          time_created: 1,
          time_updated: 1,
          data: { time: { created: 1 }, text: "Existing history must remain unchanged" },
        })
        .run()
        .pipe(Effect.orDie)
      yield* Bus.reserveSequence(database.db, sessionID, 0)
      const history = yield* database.db.select().from(SessionMessageTable).all().pipe(Effect.orDie)
      yield* seedInbox(database, sessionID, ["steer", "queue"])
      const before = yield* database.db.select().from(SessionInboxTable).all().pipe(Effect.orDie)
      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID }) =>
        Effect.sync(() => {
          drained.push(sessionID)
        }),
      )
      const execution = Context.get(context, SessionExecution.Service)
      expect(yield* execution.interrupt(sessionID, { reason: "inactivity" })).toBe(false)
      expect((yield* claims(database))[sessionID]).toBe(true)
      expect(yield* execution.interrupt(sessionID)).toBe(true)
      expect(yield* execution.interrupt(sessionID)).toBe(false)
      expect((yield* claims(database))[sessionID]).toBe(false)
      expect(yield* store.countResume(sessionID)).toBeUndefined()
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      expect(drained).toEqual([])
      expect(yield* database.db.select().from(SessionInboxTable).all().pipe(Effect.orDie)).toEqual(before)
      const messages = yield* database.db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID))
        .all()
        .pipe(Effect.orDie)
      expect(messages.filter((message) => message.id === priorID)).toEqual(history)
      expect(
        messages
          .filter((message) => message.id !== priorID)
          .map((message) => ({ type: message.type, data: message.data })),
      ).toEqual([{ type: "idle", data: expect.objectContaining({ outcome: "interrupted" }) }])
    }),
  )

  it.effect(
    "does not take over live owners, unowned children, or an original claim already selected for recovery",
    () =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const live = yield* liveProcess
        const dead = yield* exitedProcess
        const ids = [
          "ses_stop_foreign_live",
          "ses_stop_recovering",
          "ses_stop_foreign_child",
          "ses_stop_self_claim",
        ].map((id) => Session.ID.make(id))
        yield* seedSessions(database, [ids[0]], { claim_pid: live, time_suspended: 1 })
        yield* seedSessions(database, [ids[1]], { claim_pid: dead, time_suspended: 1, resume_attempts: 1 })
        yield* seedSessions(database, [ids[2]], { claim_pid: dead, time_suspended: 1, parent_id: ids[0] })
        yield* seedSessions(database, [ids[3]], { claim_pid: process.pid, time_suspended: 1 })
        const before = yield* database.db.select().from(SessionTable).all().pipe(Effect.orDie)
        const scope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
        const context = yield* buildExecution(scope, () => Effect.die("must not run"))
        const execution = Context.get(context, SessionExecution.Service)
        for (const id of ids) expect(yield* execution.interrupt(id)).toBe(false)
        expect(yield* database.db.select().from(SessionTable).all().pipe(Effect.orDie)).toEqual(before)
        expect(yield* database.db.select().from(SessionMessageTable).all().pipe(Effect.orDie)).toEqual([])
      }),
  )

  it.effect("rolls back the terminal projection when the original orphan claim changes before commit", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const store = yield* SessionStore.Service
      const sessionID = Session.ID.make("ses_orphan_stop_race")
      yield* seedSessions(database, [sessionID], { claim_pid: yield* exitedProcess, time_suspended: 123 })
      const before = yield* database.db.select().from(SessionTable).all().pipe(Effect.orDie)
      // Exercise the real event transaction. An unexpected claim change inside
      // another projector must roll back both the idle row and the release.
      yield* bus.project(SessionEvent.Execution.Interrupted, () => store.claim(sessionID))
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () => Effect.die("must not run"))
      expect(yield* Context.get(context, SessionExecution.Service).interrupt(sessionID)).toBe(false)
      expect(yield* database.db.select().from(SessionTable).all().pipe(Effect.orDie)).toEqual(before)
      expect(yield* database.db.select().from(SessionMessageTable).all().pipe(Effect.orDie)).toEqual([])
    }),
  )

  it.effect("fails closed on indeterminate owners and a PID becoming live before the cancellation commits", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_orphan_stop_owner_probe")
      const pid = yield* exitedProcess
      yield* seedSessions(database, [sessionID], { claim_pid: pid, time_suspended: 123 })
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () => Effect.die("must not run"))
      const execution = Context.get(context, SessionExecution.Service)
      for (const code of ["EPERM", "EIO"]) {
        const probe = spyOn(process, "kill").mockImplementation(() => {
          throw Object.assign(new Error("probe unavailable"), { code })
        })
        try {
          expect(yield* execution.interrupt(sessionID)).toBe(false)
        } finally {
          probe.mockRestore()
        }
      }
      const probe = spyOn(process, "kill")
        .mockImplementationOnce(() => {
          throw Object.assign(new Error("absent"), { code: "ESRCH" })
        })
        .mockImplementation(() => true)
      try {
        expect(yield* execution.interrupt(sessionID)).toBe(false)
      } finally {
        probe.mockRestore()
      }
      expect((yield* claims(database))[sessionID]).toBe(true)
      expect(yield* database.db.select().from(SessionMessageTable).all().pipe(Effect.orDie)).toEqual([])
    }),
  )

  it.effect("recovery cleans a stopped orphan's dead shell marker without resuming or redelivering input", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const kv = yield* KV.Service
      const jobs = yield* Job.Service
      const sessionID = Session.ID.make("ses_stopped_orphan_shell")
      const notificationID = SessionMessage.ID.make("msg_stopped_orphan_shell")
      const dead = yield* exitedProcess
      yield* seedSessions(database, [sessionID], { claim_pid: dead, time_suspended: 123 })
      yield* seedInbox(database, sessionID, ["queue"])
      yield* Bus.reserveSequence(database.db, sessionID, 1)
      const before = yield* database.db.select().from(SessionInboxTable).all().pipe(Effect.orDie)
      yield* kv.set(`job.background/${notificationID}`, {
        id: "sh_stopped_orphan",
        notificationID,
        status: "running",
        pid: dead,
        recovery: { kind: "shell", sessionID, shellID: "sh_stopped_orphan", command: "fixture-only" },
      })
      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID }) =>
        Effect.sync(() => {
          drained.push(sessionID)
        }),
      )
      expect(yield* Context.get(context, SessionExecution.Service).interrupt(sessionID)).toBe(true)
      const restart = Context.get(context, SessionRestart.Service)
      yield* restart.resumeSuspendedSessions
      expect(drained).toEqual([])
      expect(yield* jobs.pendingBackground).toEqual([])
      const after = yield* database.db.select().from(SessionInboxTable).all().pipe(Effect.orDie)
      expect(after.filter((row) => row.id !== notificationID)).toEqual(before)
      expect(after.find((row) => row.id === notificationID)).toMatchObject({
        type: "synthetic",
        delivery: "steer",
        payload: { metadata: { source: "shell", state: "cancelled" } },
      })
      yield* restart.resumeSuspendedSessions
      expect(yield* database.db.select().from(SessionInboxTable).all().pipe(Effect.orDie)).toEqual(after)
      expect(drained).toEqual([])
    }),
  )

  it.effect("does not resume a user-cancelled background child whose notification was not admitted", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const parent = Session.ID.make("ses_cancelled_background_parent")
      const child = Session.ID.make("ses_cancelled_background_child")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], { parent_id: parent })

      const running = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const jobs = yield* Job.make.pipe(Scope.provide(scope))
      const context = yield* buildExecution(
        scope,
        () => Deferred.succeed(running, undefined).pipe(Effect.andThen(Effect.never)),
        undefined,
        jobs,
      )
      const execution = Context.get(context, SessionExecution.Service)
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "general",
          description: "Cancelled inspection",
        },
        run: execution.resume(child).pipe(Effect.as("unused")),
      })
      yield* jobs.background(child)
      yield* Deferred.await(running)
      expect(yield* execution.interrupt(child)).toBeTrue()
      yield* execution.awaitIdle(child)
      expect((yield* jobs.wait({ id: child })).info?.status).toBe("cancelled")
      expect(yield* jobs.pendingBackground).toMatchObject([{ id: child, status: "cancelled" }])
      expect((yield* claims(database))[child]).toBe(false)
      yield* Scope.close(scope, Exit.void)

      const restartedScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(restartedScope, Exit.void))
      const restartedJobs = yield* Job.make.pipe(Scope.provide(restartedScope))
      const drained: Session.ID[] = []
      const restarted = yield* buildExecution(
        restartedScope,
        ({ sessionID }) => Effect.sync(() => void drained.push(sessionID)),
        undefined,
        restartedJobs,
      )
      yield* Context.get(restarted, SessionRestart.Service).resumeSuspendedSessions
      yield* Context.get(restarted, SessionExecution.Service).awaitIdle(parent)
      expect(drained).toEqual([parent])
      expect(yield* SessionInbox.list(database.db, parent)).toMatchObject([
        { payload: { text: expect.stringContaining("Subagent cancelled"), metadata: { state: "cancelled" } } },
      ])
      expect(yield* restartedJobs.pendingBackground).toEqual([])
    }),
  )

  it.effect("starts every claimed execution without waiting for earlier drains to finish", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionIDs = Array.from({ length: 5 }, (_, index) => Session.ID.make(`ses_resume_concurrent_${index}`))
      yield* seedSessions(database, sessionIDs, { time_suspended: Date.now() })

      const fourStarted = yield* Deferred.make<void>()
      const started: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID }) =>
        Effect.sync(() => {
          started.push(sessionID)
          if (started.length === 4) Deferred.doneUnsafe(fourStarted, Effect.void)
        }).pipe(Effect.andThen(Effect.never)),
      )
      const execution = Context.get(context, SessionExecution.Service)
      const restart = Context.get(context, SessionRestart.Service)
      yield* restart.resumeSuspendedSessions.pipe(Effect.forkIn(scope))
      yield* Deferred.await(fourStarted)

      expect([...(yield* execution.active)].toSorted()).toEqual(sessionIDs.toSorted())
    }),
  )

  it.effect("resumes each claimed Session at most once", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const first = Session.ID.make("ses_resume_first")
      const second = Session.ID.make("ses_resume_second")
      yield* seedSessions(database, [first, second], { time_suspended: Date.now() })

      const drained: string[] = []
      const bothDraining = yield* Deferred.make<void>()
      const continued: SessionEvent.Synthetic[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID }) =>
        Effect.sync(() => {
          drained.push(sessionID)
          if (drained.length === 2) Deferred.doneUnsafe(bothDraining, Effect.void)
        }),
      )
      const execution = Context.get(context, SessionExecution.Service)
      const restart = Context.get(context, SessionRestart.Service)
      yield* bus.project(SessionEvent.Synthetic, (event) => Effect.sync(() => void continued.push(event)))

      // The sweep forks resumed drains, so completion is observed through the executions.
      yield* restart.resumeSuspendedSessions
      yield* Deferred.await(bothDraining)
      yield* Effect.forEach([first, second], execution.awaitIdle, { discard: true })
      expect(drained.toSorted()).toEqual([first, second])
      expect(continued.map((event) => event.data).toSorted((a, b) => a.sessionID.localeCompare(b.sessionID))).toEqual(
        [first, second].map((sessionID) => ({
          sessionID,
          text: "The server restarted while you were working. Continue from where you left off without repeating completed work.",
          description: "Continuing after restart",
        })),
      )
      // Drains completed naturally, so claims are released and counters reset.
      expect(yield* claims(database)).toEqual({ [first]: false, [second]: false })
      expect(yield* attempts(database, first)).toBe(0)

      yield* restart.resumeSuspendedSessions
      expect(drained.length).toBe(2)
      expect(continued.length).toBe(2)
      yield* Scope.close(scope, Exit.void)
    }),
  )

  it.effect("terminalizes a turn that exhausts its resume budget instead of crash-looping", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const sessionID = Session.ID.make("ses_resume_exhausted")
      // A claim from a dead process, already resumed twice without completing.
      yield* seedSessions(database, [sessionID], { time_suspended: Date.now(), resume_attempts: 2 })

      const drained: string[] = []
      const failures: SessionEvent.Execution.Failed[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID: id }) => Effect.sync(() => void drained.push(id)), {
        maxAttempts: 2,
      })
      const restart = Context.get(context, SessionRestart.Service)
      yield* bus.project(SessionEvent.Execution.Failed, (event) => Effect.sync(() => void failures.push(event)))

      yield* restart.resumeSuspendedSessions
      expect(drained).toEqual([])
      expect(failures.map((event) => event.data.error.type)).toEqual(["aborted"])
      // The terminal released the claim and reset the counter atomically.
      expect(yield* claims(database)).toEqual({ [sessionID]: false })
      expect(yield* attempts(database, sessionID)).toBe(0)
    }),
  )

  it.effect("counts every resume durably and never consumes the claim it recovers", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_resume_counted")
      yield* seedSessions(database, [sessionID], { time_suspended: Date.now() })

      const draining = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      // The drain never terminalizes (mirrors a process that will die mid-turn).
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never)),
      )
      const restart = Context.get(context, SessionRestart.Service)
      yield* restart.resumeSuspendedSessions.pipe(Effect.forkIn(scope))
      yield* Deferred.await(draining)

      // The attempt is durable before the drain runs, and the claim is held
      // throughout: a crash anywhere in the resume path leaves both intact.
      expect(yield* attempts(database, sessionID)).toBe(1)
      expect((yield* claims(database))[sessionID]).toBe(true)

      // Teardown (a graceful shutdown's interrupt) preserves both, so the next
      // boot counts attempt 2 against the same turn.
      yield* Scope.close(scope, Exit.void)
      expect((yield* claims(database))[sessionID]).toBe(true)
      expect(yield* attempts(database, sessionID)).toBe(1)
    }),
  )

  it.effect("the sweep leaves Sessions already draining in this process untouched", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const sessionID = Session.ID.make("ses_resume_local_active")
      yield* seedSessions(database, [sessionID])

      const draining = yield* Deferred.make<void>()
      const continued: SessionEvent.Synthetic[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never)),
      )
      const execution = Context.get(context, SessionExecution.Service)
      const restart = Context.get(context, SessionRestart.Service)
      yield* bus.project(SessionEvent.Synthetic, (event) => Effect.sync(() => void continued.push(event)))

      // A live local turn holds a claim; the sweep must not count, continue, or terminalize it.
      yield* execution.resume(sessionID).pipe(Effect.forkScoped)
      yield* Deferred.await(draining)
      yield* restart.resumeSuspendedSessions

      expect(continued).toEqual([])
      expect(yield* attempts(database, sessionID)).toBe(0)
      expect((yield* claims(database))[sessionID]).toBe(true)
    }),
  )
})

describe("SessionRestart background recovery", () => {
  it.effect("keeps shell owners idle until a user prompt delivers recovered notices exactly once", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const jobs = yield* Job.Service
      const bus = yield* Bus.Service
      const parent = Session.ID.make("ses_background_recovery_parent")
      const child = Session.ID.make("ses_background_recovery_child")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], { parent_id: parent, time_suspended: Date.now() })
      yield* seedBackground(jobs, parent, [
        { id: "sh_background_orphan", shellID: "sh_background_orphan", command: "sleep 60" },
      ])
      yield* seedBackground(jobs, child, [{ id: "call-child-shell", shellID: "sh_child_orphan", command: "sleep 30" }])

      expect(yield* store.listSuspended()).toEqual([])
      expect(yield* jobs.pendingBackground).toHaveLength(2)

      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) =>
          Effect.sync(() => void drained.push(sessionID)).pipe(
            Effect.andThen(SessionInbox.promote(database.db, bus, sessionID, "steer")),
            Effect.asVoid,
          ),
        undefined,
        restarted,
      )
      const restart = Context.get(context, SessionRestart.Service)
      const execution = Context.get(context, SessionExecution.Service)
      yield* restart.resumeSuspendedSessions
      yield* Effect.forEach([parent, child], execution.awaitIdle, { discard: true })

      expect(drained).toEqual([])
      expect(yield* SessionInbox.list(database.db, parent)).toHaveLength(1)
      expect(yield* SessionInbox.list(database.db, child)).toHaveLength(1)
      expect(yield* restarted.pendingBackground).toEqual([])
      yield* restart.resumeSuspendedSessions
      expect(drained).toEqual([])
      expect(yield* SessionInbox.list(database.db, parent)).toHaveLength(1)
      expect(yield* SessionInbox.list(database.db, child)).toHaveLength(1)

      yield* seedInbox(database, parent, ["steer"])
      yield* seedInbox(database, child, ["steer"])
      yield* execution.wake(parent)
      yield* execution.wake(child)
      yield* Effect.forEach([parent, child], execution.awaitIdle, { discard: true })
      expect(drained.toSorted()).toEqual([parent, child].toSorted())
      expect((yield* store.context(parent)).filter((message) => message.type === "synthetic")).toMatchObject([
        {
          type: "synthetic",
          description: "sleep 60",
          text: expect.stringContaining("server restarted"),
          metadata: {
            source: "shell",
            jobID: "sh_background_orphan",
            shellID: "sh_background_orphan",
            state: "cancelled",
          },
        },
      ])
      expect((yield* store.context(child)).filter((message) => message.type === "synthetic")).toMatchObject([
        {
          type: "synthetic",
          metadata: {
            source: "shell",
            jobID: "call-child-shell",
            shellID: "sh_child_orphan",
            state: "cancelled",
          },
        },
      ])
      expect(yield* SessionInbox.list(database.db, parent)).toEqual([])
      expect(yield* SessionInbox.list(database.db, child)).toEqual([])
      expect(yield* claims(database)).toEqual({ [parent]: false, [child]: false })
      expect(yield* restarted.pendingBackground).toEqual([])

      yield* restart.resumeSuspendedSessions
      expect(drained).toHaveLength(2)
      expect((yield* store.context(parent)).filter((message) => message.type === "synthetic")).toHaveLength(1)
      expect((yield* store.context(child)).filter((message) => message.type === "synthetic")).toHaveLength(1)
    }),
  )

  it.effect("preserves locally running background work", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const jobs = yield* Job.Service
      const parent = Session.ID.make("ses_background_existing_parent")
      yield* seedSessions(database, [parent])
      yield* seedBackground(jobs, parent, [{ id: "call-running-shell", shellID: "sh_running", command: "sleep 60" }])

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () => Effect.void)
      const restart = Context.get(context, SessionRestart.Service)
      yield* restart.resumeSuspendedSessions

      expect((yield* store.context(parent)).filter((message) => message.type === "synthetic")).toEqual([])
      expect(yield* jobs.get("call-running-shell")).toMatchObject({ status: "running" })
      expect(yield* jobs.pendingBackground).toHaveLength(1)
    }),
  )

  it.effect("wakes the owner for a silent shell failure persisted before its completion notification", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const sessionID = Session.ID.make("ses_background_completed_shell")
      yield* seedSessions(database, [sessionID])
      const complete = yield* Deferred.make<string>()
      yield* jobs.start({
        id: "call-completed-shell",
        type: "shell",
        recovery: {
          kind: "shell",
          sessionID,
          shellID: "sh_completed",
          command: "exit 7",
        },
        run: Deferred.await(complete),
      })
      yield* jobs.background("call-completed-shell")
      yield* Deferred.succeed(complete, "Exited with code 7")
      yield* jobs.wait({ id: "call-completed-shell" })

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const drained: Session.ID[] = []
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) => Effect.sync(() => void drained.push(sessionID)),
        undefined,
        restarted,
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Context.get(context, SessionExecution.Service).awaitIdle(sessionID)

      expect(drained).toEqual([])
      const inbox = yield* SessionInbox.list(database.db, sessionID)
      expect(inbox).toMatchObject([
        {
          type: "synthetic",
          payload: {
            text: '<shell id="call-completed-shell" state="completed" command="exit 7">\nExited with code 7\n</shell>',
          },
        },
      ])
      expect(inbox[0]).toHaveProperty("payload.metadata", {
        source: "shell",
        jobID: "call-completed-shell",
        shellID: "sh_completed",
        state: "completed",
      })
      expect(yield* restarted.pendingBackground).toEqual([])
    }),
  )

  for (const delivered of [false, true]) {
    it.effect(`does not duplicate a shell notification already ${delivered ? "delivered" : "admitted"}`, () =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const bus = yield* Bus.Service
        const jobs = yield* Job.Service
        const sessions = yield* Session.Service
        const sessionID = Session.ID.make("ses_shell_notification_retry")
        yield* seedSessions(database, [sessionID])
        yield* seedBackground(jobs, sessionID, [
          { id: "call-shell-notified", shellID: "sh_notified", command: "echo done" },
        ])
        const background = (yield* jobs.pendingBackground)[0]
        if (!background) return yield* Effect.die("background record missing")
        yield* sessions.synthetic({
          id: background.notificationID,
          sessionID,
          text: "Command already completed",
          metadata: { source: "shell", shellID: "sh_notified", state: "completed" },
          resume: false,
        })
        if (delivered) yield* SessionInbox.promote(database.db, bus, sessionID, "steer")

        const scope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
        const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
        const context = yield* buildExecution(scope, () => Effect.void, undefined, restarted)
        yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

        expect(yield* restarted.pendingBackground).toEqual([])
        expect(yield* SessionInbox.list(database.db, sessionID)).toHaveLength(delivered ? 0 : 1)
        yield* SessionInbox.promote(database.db, bus, sessionID, "steer")
        const messages = (yield* sessions.messages({ sessionID })).filter((message) => message.type !== "idle")
        expect(messages).toMatchObject([
          {
            id: background.notificationID,
            type: "synthetic",
            text: "Command already completed",
            metadata: { state: "completed" },
          },
        ])
      }),
    )
  }

  it.effect("acknowledges recovery markers when their owning session is deleted", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const sessionID = Session.ID.make("ses_background_deleted")
      yield* seedSessions(database, [sessionID])
      yield* seedBackground(jobs, sessionID, [{ id: "call-deleted-shell", shellID: "sh_deleted", command: "sleep 60" }])
      yield* database.db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(scope, () => Effect.void, undefined, restarted)
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

      expect(yield* restarted.pendingBackground).toEqual([])
    }),
  )

  it.effect("delivers cancellation at the resumed parent's next step", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const store = yield* SessionStore.Service
      const bus = yield* Bus.Service
      const parent = Session.ID.make("ses_background_claimed_parent")
      yield* seedSessions(database, [parent], { time_suspended: Date.now() })
      yield* seedBackground(jobs, parent, [{ id: "call-claimed-shell", shellID: "sh_claimed", command: "sleep 60" }])

      const observed = yield* Deferred.make<string[]>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) =>
          SessionInbox.promote(database.db, bus, sessionID, "steer").pipe(
            Effect.andThen(store.context(sessionID)),
            Effect.orDie,
            Effect.flatMap((messages) =>
              Deferred.succeed(
                observed,
                messages.filter((message) => message.type === "synthetic").map((message) => message.text),
              ),
            ),
            Effect.asVoid,
          ),
        undefined,
        restarted,
      )
      const execution = Context.get(context, SessionExecution.Service)
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      expect(yield* Deferred.await(observed)).toEqual([
        "The server restarted while you were working. Continue from where you left off without repeating completed work.",
        expect.stringContaining("Command cancelled because the server restarted"),
      ])
      yield* execution.awaitIdle(parent)
      expect(yield* SessionInbox.list(database.db, parent)).toEqual([])
      expect((yield* claims(database))[parent]).toBe(false)
    }),
  )

  it.effect("does not bypass a claimed owner's restart budget for a shell notice", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const sessionID = Session.ID.make("ses_shell_recovery_exhausted")
      yield* seedSessions(database, [sessionID], { time_suspended: Date.now(), resume_attempts: 2 })
      yield* seedBackground(jobs, sessionID, [
        { id: "call-exhausted-shell", shellID: "sh_exhausted", command: "sleep 60" },
      ])

      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Scope.provide(scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) => Effect.sync(() => void drained.push(sessionID)),
        { maxAttempts: 2 },
        restarted,
      )
      const restart = Context.get(context, SessionRestart.Service)
      const execution = Context.get(context, SessionExecution.Service)
      yield* restart.resumeSuspendedSessions
      yield* execution.awaitIdle(sessionID)

      expect(drained).toEqual([])
      expect(yield* claims(database)).toEqual({ [sessionID]: false })
      expect(yield* attempts(database, sessionID)).toBe(0)
      expect(yield* SessionInbox.list(database.db, sessionID)).toMatchObject([
        { payload: { metadata: { source: "shell", state: "cancelled" } } },
      ])
      expect(yield* restarted.pendingBackground).toEqual([])
      yield* restart.resumeSuspendedSessions
      expect(drained).toEqual([])
    }),
  )

  for (const shellFirst of [false, true]) {
    for (const resumeAttempts of [1, 2]) {
      it.effect(
        `recovers a child's shell ${shellFirst ? "before" : "after"} its job record without bypassing attempt ${resumeAttempts + 1}`,
        () =>
          Effect.gen(function* () {
            const database = yield* Database.Service
            const jobs = yield* Job.Service
            const bus = yield* Bus.Service
            const store = yield* SessionStore.Service
            const parent = Session.ID.make("ses_shell_child_parent")
            const child = Session.ID.make("ses_shell_child")
            yield* seedSessions(database, [parent])
            yield* seedSessions(database, [child], {
              parent_id: parent,
              time_suspended: Date.now(),
              resume_attempts: resumeAttempts,
            })
            const shell = seedBackground(jobs, child, [
              { id: "call-child-shell", shellID: "sh_child", command: "sleep 60" },
            ])
            if (shellFirst) yield* shell
            yield* jobs.start({
              id: child,
              type: "subagent",
              recovery: {
                kind: "subagent",
                parentSessionID: parent,
                childSessionID: child,
                agent: "explore",
                description: "Inspect recovery",
              },
              run: Effect.never,
            })
            yield* jobs.background(child)
            if (!shellFirst) yield* shell
            expect((yield* jobs.pendingBackground).map((job) => job.recovery.kind)).toEqual(
              shellFirst ? ["shell", "subagent"] : ["subagent", "shell"],
            )

            const observed = yield* Deferred.make<{ attempts: number | undefined; notices: string[] }>()
            const release = yield* Deferred.make<void>()
            const parentWoken = yield* Deferred.make<void>()
            const drained: Session.ID[] = []
            const scope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
            const restarted = yield* Job.make.pipe(Scope.provide(scope))
            const context = yield* buildExecution(
              scope,
              ({ sessionID }) =>
                Effect.gen(function* () {
                  drained.push(sessionID)
                  yield* SessionInbox.promote(database.db, bus, sessionID, "steer")
                  if (sessionID === parent) {
                    yield* Deferred.succeed(parentWoken, undefined)
                    return
                  }
                  yield* Deferred.succeed(observed, {
                    attempts: yield* attempts(database, sessionID),
                    notices: (yield* store.context(sessionID))
                      .filter((message) => message.type === "synthetic")
                      .map((message) => message.text),
                  })
                  yield* Deferred.await(release)
                }),
              { maxAttempts: 2 },
              restarted,
            )
            const restart = Context.get(context, SessionRestart.Service)
            const execution = Context.get(context, SessionExecution.Service)
            yield* restart.resumeSuspendedSessions

            if (resumeAttempts < 2) {
              expect(yield* Deferred.await(observed)).toEqual({
                attempts: 2,
                notices: [
                  expect.stringContaining("The server restarted while you were working"),
                  expect.stringContaining("Command cancelled because the server restarted"),
                ],
              })
              expect(drained).toEqual([child])
              expect(yield* execution.active).not.toContain(parent)
              expect(yield* SessionInbox.list(database.db, parent)).toEqual([])
            }
            yield* Deferred.succeed(release, undefined)
            yield* Deferred.await(parentWoken)
            yield* Effect.forEach([parent, child], execution.awaitIdle, { discard: true })
            expect(drained).toEqual(resumeAttempts < 2 ? [child, parent] : [parent])
            expect((yield* store.context(parent)).filter((message) => message.type === "synthetic")).toMatchObject([
              { metadata: { source: "subagent", childID: child, state: resumeAttempts < 2 ? "completed" : "error" } },
            ])
            expect(yield* claims(database)).toEqual({ [parent]: false, [child]: false })
            expect(yield* attempts(database, child)).toBe(0)
            expect(yield* restarted.pendingBackground).toEqual([])

            yield* restart.resumeSuspendedSessions
            expect(drained).toHaveLength(resumeAttempts < 2 ? 2 : 1)
          }),
      )
    }
  }

  it.effect("resumes a background subagent and notifies its parent exactly once", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const parent = Session.ID.make("ses_subagent_recovery_parent")
      const child = Session.ID.make("ses_subagent_recovery_child")
      const unrelated = Session.ID.make("ses_subagent_unrelated_child")
      yield* seedSessions(database, [parent], { time_suspended: Date.now(), resume_attempts: 1 })
      yield* seedSessions(database, [child, unrelated], { parent_id: parent, time_suspended: Date.now() })
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Inspect recovery",
        },
        run: Effect.never,
      })
      yield* jobs.background(child)

      const resumed = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const parentResumed = yield* Deferred.make<void>()
      const parentWoken = yield* Deferred.make<void>()
      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) =>
          Effect.gen(function* () {
            drained.push(sessionID)
            if (sessionID === child) {
              yield* Deferred.succeed(resumed, undefined)
              yield* Deferred.await(release)
              return
            }
            yield* Deferred.succeed(
              drained.filter((id) => id === parent).length === 1 ? parentResumed : parentWoken,
              undefined,
            )
          }),
        undefined,
        restarted,
      )
      const restart = Context.get(context, SessionRestart.Service)
      const execution = Context.get(context, SessionExecution.Service)
      yield* restart.resumeSuspendedSessions
      yield* Deferred.await(resumed)
      yield* Deferred.await(parentResumed)
      yield* execution.awaitIdle(parent)

      yield* restart.resumeSuspendedSessions
      expect(drained.toSorted()).toEqual([child, parent].toSorted())
      expect(yield* claims(database)).toEqual({ [parent]: false, [child]: true, [unrelated]: false })
      expect(yield* attempts(database, child)).toBe(1)
      expect(yield* restarted.get(child)).toMatchObject({ status: "running" })

      yield* Deferred.succeed(release, undefined)
      yield* Deferred.await(parentWoken)
      expect(drained.filter((id) => id === child)).toHaveLength(1)
      expect(drained.filter((id) => id === parent)).toHaveLength(2)
      expect(yield* SessionInbox.list(database.db, parent)).toMatchObject([
        {
          payload: {
            description: "Inspect recovery",
            metadata: { source: "subagent", childID: child, agent: "explore", state: "completed" },
          },
        },
      ])
      expect(yield* restarted.pendingBackground).toEqual([])
      yield* restart.resumeSuspendedSessions
      expect(yield* SessionInbox.list(database.db, parent)).toHaveLength(1)
    }),
  )

  it.effect("delivers a subagent result persisted before restart without rerunning the child", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const parent = Session.ID.make("ses_subagent_completed_parent")
      const child = Session.ID.make("ses_subagent_completed_child")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], { parent_id: parent })
      const complete = yield* Deferred.make<string>()
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Completed inspection",
        },
        run: Deferred.await(complete),
      })
      yield* jobs.background(child)
      yield* Deferred.succeed(complete, "Recovered result")
      yield* jobs.wait({ id: child })

      const parentWoken = yield* Deferred.make<void>()
      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) =>
          Effect.sync(() => void drained.push(sessionID)).pipe(
            Effect.andThen(Deferred.succeed(parentWoken, undefined)),
          ),
        undefined,
        restarted,
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Deferred.await(parentWoken)

      expect(drained).toEqual([parent])
      expect(yield* SessionInbox.list(database.db, parent)).toMatchObject([
        { payload: { text: expect.stringContaining("Recovered result"), metadata: { state: "completed" } } },
      ])
      expect(yield* restarted.pendingBackground).toEqual([])
    }),
  )

  it.effect("retains a subagent completion marker when synthetic admission conflicts", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const admission = yield* SessionInbox.Service
      const jobs = yield* Job.Service
      const sessions = yield* Session.Service
      const parent = Session.ID.make("ses_completion_conflict_parent")
      const child = Session.ID.make("ses_completion_conflict_child")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], { parent_id: parent })
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Completed inspection",
        },
        run: Effect.succeed("Recovered result"),
      })
      yield* jobs.wait({ id: child })
      yield* jobs.background(child)
      const marker = (yield* jobs.pendingBackground)[0]
      if (!marker) return yield* Effect.die("background record missing")
      yield* admission.admit({
        id: marker.notificationID,
        sessionID: parent,
        item: { type: "user", payload: { text: "User input" }, delivery: "steer" },
      })

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () => Effect.die("Admission must not wake the parent"))
      const exit = yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions.pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(Session.SyntheticConflictError)
      expect(yield* jobs.pendingBackground).toEqual([marker])
      expect(yield* sessions.inbox(parent)).toMatchObject([{ type: "user", payload: { text: "User input" } }])
    }),
  )

  for (const resumeAttempts of [1, 2]) {
    it.effect(`honors a suspended parent's restart budget after ${resumeAttempts} attempts before notifying it`, () =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const bus = yield* Bus.Service
        const jobs = yield* Job.Service
        const parent = Session.ID.make("ses_subagent_budget_parent")
        const children = [
          Session.ID.make("ses_subagent_budget_child_1"),
          Session.ID.make("ses_subagent_budget_child_2"),
        ]
        yield* seedSessions(database, [parent], { time_suspended: Date.now(), resume_attempts: resumeAttempts })
        yield* seedSessions(database, children, { parent_id: parent })
        const complete = yield* Deferred.make<string>()
        for (const child of children) {
          yield* jobs.start({
            id: child,
            type: "subagent",
            recovery: {
              kind: "subagent",
              parentSessionID: parent,
              childSessionID: child,
              agent: "explore",
              description: "Completed inspection",
            },
            run: Deferred.await(complete),
          })
          yield* jobs.background(child)
        }
        yield* Deferred.succeed(complete, "Recovered result")
        yield* Effect.forEach(children, (id) => jobs.wait({ id }), { discard: true })

        const draining = yield* Deferred.make<number | undefined>()
        const release = yield* Deferred.make<void>()
        const drained: Session.ID[] = []
        const continued: Session.ID[] = []
        yield* bus.project(SessionEvent.Synthetic, (event) =>
          Effect.sync(() => void continued.push(event.data.sessionID)),
        )
        const scope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
        const restarted = yield* Job.make.pipe(Scope.provide(scope))
        const context = yield* buildExecution(
          scope,
          ({ sessionID }) =>
            Effect.gen(function* () {
              drained.push(sessionID)
              yield* Deferred.succeed(draining, yield* attempts(database, sessionID))
              yield* Deferred.await(release)
            }),
          { maxAttempts: 2 },
          restarted,
        )
        const restart = Context.get(context, SessionRestart.Service)
        const execution = Context.get(context, SessionExecution.Service)
        yield* restart.resumeSuspendedSessions

        if (resumeAttempts < 2) {
          expect(yield* Deferred.await(draining)).toBe(2)
          expect(drained).toEqual([parent])
          expect(continued).toEqual([parent])
          yield* Deferred.succeed(release, undefined)
          yield* execution.awaitIdle(parent)
        }
        if (resumeAttempts === 2) {
          expect(drained).toEqual([])
          expect(continued).toEqual([])
        }
        expect((yield* claims(database))[parent]).toBe(false)
        expect(yield* attempts(database, parent)).toBe(0)
        expect(yield* SessionInbox.list(database.db, parent)).toHaveLength(2)
        expect(yield* restarted.pendingBackground).toEqual([])
        yield* restart.resumeSuspendedSessions
        expect(drained).toHaveLength(resumeAttempts < 2 ? 1 : 0)
      }),
    )
  }

  it.effect("terminalizes a recovered subagent that exhausts its resume budget", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const jobs = yield* Job.Service
      const parent = Session.ID.make("ses_subagent_exhausted_parent")
      const child = Session.ID.make("ses_subagent_exhausted_child")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], { parent_id: parent, time_suspended: Date.now(), resume_attempts: 2 })
      yield* jobs.start({
        id: child,
        type: "subagent",
        recovery: {
          kind: "subagent",
          parentSessionID: parent,
          childSessionID: child,
          agent: "explore",
          description: "Exhausted inspection",
        },
        run: Effect.never,
      })
      yield* jobs.background(child)

      const parentWoken = yield* Deferred.make<void>()
      const drained: Session.ID[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID }) =>
          Effect.sync(() => void drained.push(sessionID)).pipe(
            Effect.andThen(Deferred.succeed(parentWoken, undefined)),
          ),
        { maxAttempts: 2 },
        restarted,
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Deferred.await(parentWoken)

      expect(drained).toEqual([parent])
      expect((yield* claims(database))[child]).toBe(false)
      expect(yield* SessionInbox.list(database.db, parent)).toMatchObject([
        {
          payload: {
            text: expect.stringContaining("will not be resumed automatically"),
            metadata: { source: "subagent", childID: child, state: "error" },
          },
        },
      ])
      expect(yield* restarted.pendingBackground).toEqual([])
    }),
  )
})

describe("SessionExecution interrupt continuation", () => {
  it.effect("resumes only steering input after an interrupt with continue", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_continue_steer")
      yield* seedSessions(database, [sessionID])
      yield* seedInbox(database, sessionID, ["steer", "queue"])

      const draining = yield* Deferred.make<void>()
      const drains: Array<{ force: boolean; promotable?: SessionInbox.Promotable }> = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, (input) =>
        Effect.suspend(() => {
          drains.push({ force: input.force, promotable: input.promotable })
          if (drains.length > 1) return Effect.void
          return Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never))
        }),
      )
      const execution = Context.get(context, SessionExecution.Service)
      yield* execution.resume(sessionID).pipe(Effect.forkScoped)
      yield* Deferred.await(draining)

      yield* execution.interrupt(sessionID, { resume: true })
      yield* execution.awaitIdle(sessionID)

      // The successor drain is steer-scoped: queued next-turn work stays parked.
      expect(drains).toEqual([
        { force: true, promotable: "input" },
        { force: false, promotable: "steer" },
      ])
    }),
  )

  it.effect("stays parked after an interrupt with continue when only queued work remains", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_continue_parked")
      yield* seedSessions(database, [sessionID])
      yield* seedInbox(database, sessionID, ["queue"])

      const draining = yield* Deferred.make<void>()
      const drains: Array<SessionInbox.Promotable | undefined> = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, (input) =>
        Effect.suspend(() => {
          drains.push(input.promotable)
          return Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never))
        }),
      )
      const execution = Context.get(context, SessionExecution.Service)
      yield* execution.resume(sessionID).pipe(Effect.forkScoped)
      yield* Deferred.await(draining)

      yield* execution.interrupt(sessionID, { resume: true })
      yield* execution.awaitIdle(sessionID)

      expect(drains).toEqual(["input"])
      expect(yield* execution.active).toEqual(new Set())
    }),
  )

  it.effect("an idle interrupt with continue resumes pending steers", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_continue_idle")
      yield* seedSessions(database, [sessionID])
      yield* seedInbox(database, sessionID, ["steer"])

      const drains: Array<{ force: boolean; promotable?: SessionInbox.Promotable }> = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, (input) =>
        Effect.sync(() => void drains.push({ force: input.force, promotable: input.promotable })),
      )
      const execution = Context.get(context, SessionExecution.Service)

      yield* execution.interrupt(sessionID, { resume: true })
      yield* execution.awaitIdle(sessionID)

      expect(drains).toEqual([{ force: false, promotable: "steer" }])
    }),
  )

  it.effect("an interrupt with continue resumes a queued compaction next in line", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_continue_compaction")
      yield* seedSessions(database, [sessionID])
      yield* seedInbox(database, sessionID, [{ delivery: "queue", type: "compaction" }])

      const draining = yield* Deferred.make<void>()
      const drains: Array<{ force: boolean; promotable?: SessionInbox.Promotable }> = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, (input) =>
        Effect.suspend(() => {
          drains.push({ force: input.force, promotable: input.promotable })
          if (drains.length > 1) return Effect.void
          return Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never))
        }),
      )
      const execution = Context.get(context, SessionExecution.Service)
      yield* execution.resume(sessionID).pipe(Effect.forkScoped)
      yield* Deferred.await(draining)

      yield* execution.interrupt(sessionID, { resume: true })
      yield* execution.awaitIdle(sessionID)

      // Control work is housekeeping, not next-turn input: continue runs it.
      expect(drains).toEqual([
        { force: true, promotable: "input" },
        { force: false, promotable: "steer" },
      ])
    }),
  )

  it.effect("keeps a control item parked behind a queued prompt on continue", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_continue_control_behind")
      yield* seedSessions(database, [sessionID])
      yield* seedInbox(database, sessionID, ["queue", { delivery: "queue", type: "compaction" }])

      const drains: Array<{ force: boolean; promotable?: SessionInbox.Promotable }> = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, (input) =>
        Effect.sync(() => void drains.push({ force: input.force, promotable: input.promotable })),
      )
      const execution = Context.get(context, SessionExecution.Service)

      yield* execution.interrupt(sessionID, { resume: true })
      yield* execution.awaitIdle(sessionID)

      // The queued prompt is next in line; the compaction behind it waits its turn.
      expect(drains).toEqual([])
    }),
  )
})

describe("SessionRestart claim ownership", () => {
  it.effect("stamps the execution claim with the owning process", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_claim_owner_stamp")
      yield* seedSessions(database, [sessionID])

      const draining = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never)),
      )
      yield* Context.get(context, SessionExecution.Service).resume(sessionID).pipe(Effect.forkIn(scope))
      yield* Deferred.await(draining)

      expect(yield* claimOwner(database, sessionID)).toBe(process.pid)
    }),
  )

  it.effect("the sweep leaves claims held by another live process untouched", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      const sessionID = Session.ID.make("ses_claim_owner_live")
      const owner = yield* liveProcess
      yield* seedSessions(database, [sessionID], { time_suspended: Date.now(), claim_pid: owner })

      const drained: string[] = []
      const continued: SessionEvent.Synthetic[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID: id }) => Effect.sync(() => void drained.push(id)))
      yield* bus.project(SessionEvent.Synthetic, (event) => Effect.sync(() => void continued.push(event)))
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

      expect(drained).toEqual([])
      expect(continued).toEqual([])
      expect(yield* attempts(database, sessionID)).toBe(0)
      expect((yield* claims(database))[sessionID]).toBe(true)
    }),
  )

  it.effect("the sweep resumes claims whose owning process has exited", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_claim_owner_dead")
      yield* seedSessions(database, [sessionID], { time_suspended: Date.now(), claim_pid: yield* exitedProcess })

      const drained = yield* Deferred.make<string>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID: id }) => Deferred.succeed(drained, id))
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

      expect(yield* Deferred.await(drained)).toBe(sessionID)
    }),
  )

  it.effect("a resumed claim is re-stamped with the resuming process", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_claim_owner_restamp")
      const claimed = Date.now() - 60_000
      yield* seedSessions(database, [sessionID], { time_suspended: claimed, claim_pid: yield* exitedProcess })

      const draining = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () =>
        Deferred.succeed(draining, undefined).pipe(Effect.andThen(Effect.never)),
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Deferred.await(draining)

      expect(yield* claimOwner(database, sessionID)).toBe(process.pid)
      const row = yield* database.db
        .select({ claimed: SessionTable.time_suspended })
        .from(SessionTable)
        .where(eq(SessionTable.id, sessionID))
        .get()
        .pipe(Effect.orDie)
      expect(row?.claimed).toBe(claimed)
    }),
  )

  it.effect("the sweep leaves background jobs owned by another live process pending", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const kv = yield* KV.Service
      const sessionID = Session.ID.make("ses_background_owner_live")
      const owner = yield* liveProcess
      yield* seedSessions(database, [sessionID])
      const notificationID = SessionMessage.ID.create()
      yield* kv.set(`job.background/${notificationID}`, {
        id: "sh_background_owner_live",
        notificationID,
        recovery: { kind: "shell", sessionID, shellID: "sh_background_owner_live", command: "sleep 60" },
        status: "running",
        pid: owner,
      })

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(scope, () => Effect.void, undefined, restarted)
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

      expect((yield* restarted.pendingBackground).map((job) => job.id)).toEqual(["sh_background_owner_live"])
      expect((yield* store.context(sessionID)).filter((message) => message.type === "synthetic")).toEqual([])
    }),
  )

  it.effect("persists the owning process on recoverable background jobs", () =>
    Effect.gen(function* () {
      const jobs = yield* Job.Service
      const sessionID = Session.ID.make("ses_background_owner_stamp")
      yield* seedBackground(jobs, sessionID, [{ id: "sh_owner_stamp", shellID: "sh_owner_stamp", command: "sleep 60" }])

      expect((yield* jobs.pendingBackground).map((job) => job.pid)).toEqual([process.pid])
    }),
  )

  it.effect("the sweep keeps child claims held by another live process", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const parent = Session.ID.make("ses_claim_owner_child_parent")
      const child = Session.ID.make("ses_claim_owner_child_live")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], {
        parent_id: parent,
        time_suspended: Date.now(),
        claim_pid: yield* liveProcess,
      })

      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, () => Effect.void)
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

      expect((yield* claims(database))[child]).toBe(true)
    }),
  )

  it.effect("treats pid 1 as an unknown owner and resumes the claim", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = Session.ID.make("ses_claim_owner_pid_one")
      yield* seedSessions(database, [sessionID], { time_suspended: Date.now(), claim_pid: 1 })

      const drained = yield* Deferred.make<string>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID: id }) => Deferred.succeed(drained, id))
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions

      expect(yield* Deferred.await(drained)).toBe(sessionID)
    }),
  )

  it.effect("resumes only the orphaned claims when a live owner holds others", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const orphaned = Session.ID.make("ses_claim_owner_mixed_dead")
      const held = Session.ID.make("ses_claim_owner_mixed_live")
      yield* seedSessions(database, [orphaned], { time_suspended: Date.now(), claim_pid: yield* exitedProcess })
      yield* seedSessions(database, [held], { time_suspended: Date.now(), claim_pid: yield* liveProcess })

      const drained: string[] = []
      const orphanedDrained = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const context = yield* buildExecution(scope, ({ sessionID: id }) =>
        Effect.sync(() => void drained.push(id)).pipe(
          Effect.andThen(id === orphaned ? Deferred.succeed(orphanedDrained, undefined) : Effect.void),
        ),
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Deferred.await(orphanedDrained)

      expect(drained).toEqual([orphaned])
      expect(yield* attempts(database, held)).toBe(0)
      expect((yield* claims(database))[held]).toBe(true)
    }),
  )

  it.effect("skips subagent jobs whose child is held by another live process", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const kv = yield* KV.Service
      const parent = Session.ID.make("ses_subagent_owner_parent")
      const child = Session.ID.make("ses_subagent_owner_child_live")
      yield* seedSessions(database, [parent])
      yield* seedSessions(database, [child], {
        parent_id: parent,
        time_suspended: Date.now(),
        claim_pid: yield* liveProcess,
      })
      yield* seedSubagentJob(kv, parent, child, "running")

      const drained: string[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID: id }) => Effect.sync(() => void drained.push(id)),
        undefined,
        restarted,
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Context.get(context, SessionExecution.Service).awaitIdle(child)

      expect(drained).toEqual([])
      expect(yield* attempts(database, child)).toBe(0)
      expect((yield* restarted.pendingBackground).map((job) => job.id)).toEqual(["call-subagent-owner"])
    }),
  )

  it.effect("leaves subagent results for a parent held by another live process undelivered", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const kv = yield* KV.Service
      const store = yield* SessionStore.Service
      const parent = Session.ID.make("ses_subagent_owner_parent_live")
      const child = Session.ID.make("ses_subagent_owner_child_done")
      yield* seedSessions(database, [parent], { time_suspended: Date.now(), claim_pid: yield* liveProcess })
      yield* seedSessions(database, [child], { parent_id: parent })
      yield* seedSubagentJob(kv, parent, child, "completed")

      const drained: string[] = []
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
      const restarted = yield* Job.make.pipe(Effect.provideService(Scope.Scope, scope))
      const context = yield* buildExecution(
        scope,
        ({ sessionID: id }) => Effect.sync(() => void drained.push(id)),
        undefined,
        restarted,
      )
      yield* Context.get(context, SessionRestart.Service).resumeSuspendedSessions
      yield* Context.get(context, SessionExecution.Service).awaitIdle(parent)

      expect(drained).toEqual([])
      expect((yield* store.context(parent)).filter((message) => message.type === "synthetic")).toEqual([])
      expect((yield* restarted.pendingBackground).map((job) => job.id)).toEqual(["call-subagent-owner"])
    }),
  )
})

/** A subagent job persisted without a pid, as servers from before claim ownership write them. */
function seedSubagentJob(kv: KV.Interface, parent: Session.ID, child: Session.ID, status: "running" | "completed") {
  const notificationID = SessionMessage.ID.create()
  return kv.set(`job.background/${notificationID}`, {
    id: "call-subagent-owner",
    notificationID,
    recovery: {
      kind: "subagent",
      parentSessionID: parent,
      childSessionID: child,
      agent: "general",
      description: "review",
    },
    status,
    ...(status === "completed" ? { output: "done" } : {}),
  })
}

const liveProcess = Effect.acquireRelease(
  Effect.sync(() => Bun.spawn([process.execPath, "-e", "setTimeout(() => {}, 60_000)"])),
  (child) => Effect.sync(() => child.kill()),
).pipe(Effect.map((child) => child.pid))

const exitedProcess = Effect.promise(async () => {
  const child = Bun.spawn([process.execPath, "-e", ""])
  await child.exited
  return child.pid
})

function claimOwner(database: Database.Service["Service"], sessionID: Session.ID) {
  return database.db
    .select({ pid: SessionTable.claim_pid })
    .from(SessionTable)
    .where(eq(SessionTable.id, sessionID))
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => row?.pid),
    )
}

function seedBackground(
  jobs: Job.Interface,
  sessionID: Session.ID,
  background: ReadonlyArray<{ readonly id: string; readonly shellID: string; readonly command: string }>,
) {
  return Effect.forEach(
    background,
    (job) =>
      Effect.gen(function* () {
        yield* jobs.start({
          id: job.id,
          type: "shell",
          recovery: { kind: "shell", sessionID, shellID: job.shellID, command: job.command },
          run: Effect.never,
        })
        yield* jobs.background(job.id)
      }),
    { discard: true },
  )
}

/** Plain deliveries seed user prompts; objects seed control items. */
function seedInbox(
  database: Database.Service["Service"],
  sessionID: Session.ID,
  items: ReadonlyArray<
    SessionInbox.Delivery | { readonly delivery: SessionInbox.Delivery; readonly type: "compaction" }
  >,
) {
  return database.db
    .insert(SessionInboxTable)
    .values(
      items.map((item, index) => {
        const entry = typeof item === "string" ? { delivery: item, type: "user" as const } : item
        return {
          id: SessionMessage.ID.create(),
          session_id: sessionID,
          type: entry.type,
          payload: entry.type === "user" ? { text: "queued prompt" } : {},
          delivery: entry.delivery,
          enqueued_seq: index + 1,
        }
      }),
    )
    .run()
    .pipe(Effect.orDie)
}

function seedSessions(
  database: Database.Service["Service"],
  sessionIDs: ReadonlyArray<Session.ID>,
  values: Partial<
    Pick<typeof SessionTable.$inferInsert, "time_suspended" | "claim_pid" | "resume_attempts" | "parent_id">
  > = {},
) {
  return Effect.gen(function* () {
    yield* database.db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* database.db
      .insert(SessionTable)
      .values(
        sessionIDs.map((id) => ({
          id,
          project_id: Project.ID.global,
          slug: id,
          directory: "/project",
          title: id,
          version: "test",
          ...values,
        })),
      )
      .run()
      .pipe(Effect.orDie)
  })
}

function claims(database: Database.Service["Service"]) {
  return database.db
    .select({ id: SessionTable.id, claimed: SessionTable.time_suspended })
    .from(SessionTable)
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((rows) => Object.fromEntries(rows.map((row) => [row.id, row.claimed !== null]))),
    )
}

function attempts(database: Database.Service["Service"], sessionID: Session.ID) {
  return database.db
    .select({ attempts: SessionTable.resume_attempts })
    .from(SessionTable)
    .where(eq(SessionTable.id, sessionID))
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => row?.attempts),
    )
}

/** Builds the local execution layer plus the restart actions against the test harness services. */
function buildExecution(
  scope: Scope.Closeable,
  drain: (input: Parameters<SessionRunner.Interface["drain"]>[0]) => Effect.Effect<void, SessionRunner.RunError>,
  options?: SessionRestart.Options,
  overrideJobs?: Job.Interface,
) {
  return Effect.gen(function* () {
    const database = yield* Database.Service
    const bus = yield* Bus.Service
    const store = yield* SessionStore.Service
    const jobs = overrideJobs ?? (yield* Job.Service)
    const sessions = yield* Session.Service
    const sessionLayer = Layer.effect(
      Session.Service,
      Effect.gen(function* () {
        const execution = yield* SessionExecution.Service
        return Session.Service.of({
          ...sessions,
          synthetic: (input) =>
            sessions
              .synthetic({ ...input, resume: false })
              .pipe(Effect.tap(() => (input.resume === false ? Effect.void : execution.wake(input.sessionID)))),
        })
      }),
    )
    const runner = Layer.succeed(
      SessionRunner.Service,
      SessionRunner.Service.of({
        drain: (input) => drain(input).pipe(Effect.as(SessionRunner.DrainResult.Complete())),
      }),
    )
    const locations = Layer.effect(
      LocationServiceMap.Service,
      LayerMap.make(
        () =>
          // The local execution test only needs the Session runner from the Location graph.
          // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
          runner as unknown as Layer.Layer<LocationServices>,
      ),
    )
    return yield* Layer.buildWithScope(
      SessionRestart.layer(options).pipe(
        Layer.provideMerge(sessionLayer),
        Layer.provideMerge(Layer.fresh(SessionExecution.layer)),
        Layer.provide(Layer.succeed(Database.Service, database)),
        Layer.provide(Layer.succeed(Bus.Service, bus)),
        Layer.provide(Layer.succeed(SessionStore.Service, store)),
        Layer.provide(Layer.succeed(Job.Service, jobs)),
        // Do not reuse the outer harness's selector with its already-captured Location map.
        Layer.provide(
          AppNodeBuilder.build(Instance.node, [LocationServiceMap.node.replace(locations)]).pipe(Layer.fresh),
        ),
      ),
      scope,
    )
  })
}
