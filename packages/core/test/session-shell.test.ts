import { describe, expect, setDefaultTimeout, spyOn } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Option, Schedule, Schema, Stream } from "effect"
import { Bus } from "@opencode/core/bus"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionRunCoordinator } from "@opencode/core/session/run-coordinator"
import { Shell } from "@opencode/core/shell"
import { Database } from "@opencode/core/database/database"
import { EventSequenceTable } from "@opencode/core/event/sql"
import { SessionDrain } from "@opencode/core/session/drain"
import { and, eq, gte, lt } from "drizzle-orm"
import { KVTable } from "@opencode/core/kv/sql"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { location } from "./fixture/location"
import { offlineModels } from "./fixture/models"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

// Every test boots a real Location, so shell start waits for cold plugin activation before spawning.
setDefaultTimeout(15_000)

class ExecutionControl extends Context.Service<
  ExecutionControl,
  {
    started: Deferred.Deferred<void>
    release: Deferred.Deferred<void>
    wakes: Session.ID[]
  }
>()("test/SessionShell/ExecutionControl") {}

const controlLayer = Layer.effect(
  ExecutionControl,
  Effect.gen(function* () {
    return { started: yield* Deferred.make<void>(), release: yield* Deferred.make<void>(), wakes: [] }
  }),
)

const executionLayer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const control = yield* ExecutionControl
    const coordinator = yield* SessionRunCoordinator.make<Session.ID, never>({
      drain: () => Deferred.succeed(control.started, undefined).pipe(Effect.andThen(Deferred.await(control.release))),
    })
    return SessionExecution.Service.of({
      active: coordinator.active,
      isActive: coordinator.isActive,
      resume: coordinator.run,
      interrupt: (sessionID) => coordinator.interrupt(sessionID),
      awaitIdle: coordinator.awaitIdle,
      wake: (sessionID) =>
        Effect.sync(() => control.wakes.push(sessionID)).pipe(Effect.andThen(coordinator.wake(sessionID))),
    })
  }),
)

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Bus.node, Session.node, SessionExecution.node, LocationServiceMap.node, Database.node]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      SessionExecution.node.replace(executionLayer.pipe(Layer.provide(controlLayer))),
      offlineModels,
    ],
  ).pipe(Layer.provideMerge(controlLayer)),
)

const setup = Effect.gen(function* () {
  const tmp = yield* tmpdirScoped()
  const session = yield* Session.Service
  const created = yield* session.create({ location: Location.Ref.make({ directory: AbsolutePath.make(tmp.path) }) })
  const locations = yield* LocationServiceMap.Service
  const shell = yield* Shell.Service.pipe(Effect.provide(locations.get(created.location)))
  const control = yield* ExecutionControl
  const execution = yield* SessionExecution.Service
  return { tmp, session, created, shell, control, execution }
})

const log = (session: Session.Interface, sessionID: Session.ID, follow = false) =>
  session.log({ sessionID, follow }).pipe(Stream.filter((event) => !Bus.isSynced(event)))

// File gates prove the child process is running without relying on elapsed-time assertions.
const launch = Effect.fn(function* (fixture: Effect.Success<typeof setup>, name: string) {
  const release = Effect.promise(() => Bun.write(path.join(fixture.tmp.path, `${name}.release`), "")).pipe(
    Effect.asVoid,
  )
  yield* Effect.addFinalizer(() => release)
  const command =
    process.platform === "win32"
      ? `Write-Output '${name} started'; New-Item '${name}.started' -ItemType File | Out-Null; while (!(Test-Path '${name}.release') -and (Test-Path $PWD)) { Start-Sleep -Milliseconds 10 }; Write-Output '${name} finished'`
      : `printf '${name} started\\n'; touch ${name}.started; while [ ! -f ${name}.release ] && [ -d "$PWD" ]; do sleep 0.01; done; printf '${name} finished\\n'`
  const caller = yield* fixture.session
    .shell({ sessionID: fixture.created.id, command })
    .pipe(Effect.scoped, Effect.forkScoped)
  yield* Effect.promise(() => Bun.file(path.join(fixture.tmp.path, `${name}.started`)).exists()).pipe(
    Effect.repeat({ until: (exists) => exists, schedule: Schedule.spaced("10 millis") }),
    Effect.timeout("5 seconds"),
  )
  const event = yield* log(fixture.session, fixture.created.id, true).pipe(
    Stream.filter((event): event is SessionEvent.Shell.Started => event.type === "session.shell.started"),
    Stream.filter((event) => event.data.shell.command === command),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
    Effect.timeout("5 seconds"),
  )
  return { caller, shellID: event.data.shell.id, release }
})

describe("Session.shell", () => {
  it.live("drain review: an exited leader cannot certify while its owned grandchild can still write", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const db = (yield* Database.Service).db
      const file = (name: string) => path.join(fixture.tmp.path, name)
      const waitFile = (name: string) =>
        Effect.promise(() => Bun.file(file(name)).exists()).pipe(
          Effect.repeat({ until: (exists) => exists, schedule: Schedule.spaced("10 millis") }),
          Effect.timeout("5 seconds"),
        )
      const release = Effect.promise(() => Bun.write(file("grandchild.release"), ""))
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* release
          yield* waitFile("grandchild.finished")
          const pid = Number(yield* Effect.promise(() => Bun.file(file("grandchild.pid")).text()))
          if (!Number.isSafeInteger(pid) || pid <= 1) return yield* Effect.die("invalid fixture-owned pid")
          // Only observe this fixture's reported PID; never signal other processes.
          yield* Effect.sync(() => {
            try {
              process.kill(pid, 0)
              return false
            } catch (error) {
              if (error instanceof Error && "code" in error && error.code === "ESRCH") return true
              throw error
            }
          }).pipe(
            Effect.repeat({ until: (gone) => gone, schedule: Schedule.spaced("10 millis") }),
            Effect.timeout("5 seconds"),
          )
        }).pipe(Effect.orDie),
      )
      yield* Effect.promise(() =>
        Bun.write(
          file("grandchild.mjs"),
          `
        import { spawn } from 'node:child_process';
        import { existsSync, writeFileSync } from 'node:fs';
        if (process.argv[2] !== 'child') {
          const child = spawn(process.execPath, ['grandchild.mjs', 'child'], { stdio: 'ignore' });
          child.unref(); process.exit(0);
        }
        writeFileSync('grandchild.pid', String(process.pid));
        writeFileSync('grandchild.started', '');
        const deadline = Date.now() + 10000;
        while (!existsSync('grandchild.release') && Date.now() < deadline) await Bun.sleep(10);
        if (existsSync('grandchild.release')) writeFileSync('grandchild.wrote', 'after leader exit');
        writeFileSync('grandchild.finished', '');
        process.exit(0);
      `,
        ),
      )
      const command = `${process.platform === "win32" ? "& " : ""}${JSON.stringify(process.execPath.replaceAll("\\", "/"))} grandchild.mjs`
      const child = yield* fixture.shell.create({
        command,
        cwd: fixture.tmp.path,
        metadata: { sessionID: fixture.created.id },
        timeout: 0,
      })
      yield* waitFile("grandchild.started")
      expect((yield* fixture.shell.wait(child.id)).exit).toBe(0)
      // Observe scope completion, not an elapsed-time approximation. The fixed
      // kernel retains an unresolved ticket; the old code deleted this row.
      const work = () =>
        db
          .select({ value: KVTable.value })
          .from(KVTable)
          .where(
            and(
              gte(KVTable.key, `session.work/${fixture.created.id}/`),
              lt(KVTable.key, `session.work/${fixture.created.id}/\uffff`),
            ),
          )
          .all()
          .pipe(
            Effect.orDie,
            Effect.map((rows) =>
              rows.map((row) =>
                Schema.decodeUnknownSync(
                  Schema.Struct({ kind: Schema.String, state: Schema.optionalKey(Schema.String) }),
                )(row.value),
              ),
            ),
          )
      yield* work().pipe(
        Effect.repeat({
          until: (rows) => rows.length === 0 || rows.every((row) => row.state === "unresolved"),
          schedule: Schedule.spaced("10 millis"),
        }),
        Effect.timeout("5 seconds"),
      )
      const head = yield* db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, fixture.created.id))
        .get()
        .pipe(Effect.orDie)
      if (!head) return yield* Effect.die("missing native sequence")
      const request = {
        v: 1 as const,
        sessionID: fixture.created.id,
        requestID: "00000000-0000-4000-8000-000000000091",
        generation: 0,
        sequence: head.seq,
      }
      yield* SessionDrain.begin(request)
      const early = yield* SessionDrain.complete(request).pipe(Effect.exit)
      yield* release
      yield* waitFile("grandchild.wrote")
      expect(early).toMatchObject({ _tag: "Failure" })
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.live("drain review: admitted Session.shell settlement completes under the fence without new work", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const db = (yield* Database.Service).db
      const reading = yield* Deferred.make<void>()
      const releaseRead = yield* Deferred.make<void>()
      yield* Effect.addFinalizer(() => Deferred.succeed(releaseRead, undefined))
      const output = fixture.shell.output
      const delayed = spyOn(fixture.shell, "output").mockImplementation((...args) =>
        Deferred.succeed(reading, undefined).pipe(
          Effect.andThen(Deferred.await(releaseRead)),
          Effect.andThen(output(...args)),
        ),
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => delayed.mockRestore()))
      const running = yield* launch(fixture, "settlement")
      const head = yield* db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, fixture.created.id))
        .get()
        .pipe(Effect.orDie)
      if (!head) return yield* Effect.die("missing native sequence")
      const request = {
        v: 1 as const,
        sessionID: fixture.created.id,
        requestID: "00000000-0000-4000-8000-000000000092",
        generation: 0,
        sequence: head.seq,
      }
      yield* SessionDrain.begin(request)
      yield* running.release
      yield* Deferred.await(reading).pipe(Effect.timeout("5 seconds"))
      const rows = yield* db
        .select({ value: KVTable.value })
        .from(KVTable)
        .where(
          and(
            gte(KVTable.key, `session.work/${fixture.created.id}/`),
            lt(KVTable.key, `session.work/${fixture.created.id}/\uffff`),
          ),
        )
        .all()
        .pipe(Effect.orDie)
      const early = yield* SessionDrain.complete(request).pipe(Effect.exit)
      expect(
        yield* fixture.session
          .synthetic({
            sessionID: fixture.created.id,
            text: "forged completion",
            metadata: { source: "shell", shellID: running.shellID },
            resume: false,
          })
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      yield* Deferred.succeed(releaseRead, undefined)
      expect(yield* Fiber.join(running.caller).pipe(Effect.exit)).toMatchObject({ _tag: "Success" })
      expect(early).toMatchObject({ _tag: "Failure" })
      expect(
        rows.map((row) => Schema.decodeUnknownSync(Schema.Struct({ kind: Schema.String }))(row.value).kind),
      ).toContain("session-shell")
      const messages = yield* fixture.session.messages({ sessionID: fixture.created.id, order: "asc" })
      expect(messages).toMatchObject([
        { type: "shell", shellID: running.shellID, status: "exited" },
        {
          type: "synthetic",
          text: expect.stringContaining("settlement finished"),
          metadata: { source: "shell", shellID: running.shellID },
        },
      ])
      expect(yield* fixture.session.inbox(fixture.created.id)).toEqual([])
      expect(fixture.control.wakes).toEqual([])
      const settled = yield* db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, fixture.created.id))
        .get()
        .pipe(Effect.orDie)
      expect(settled?.seq).toBe(request.sequence + 3)
      expect(
        yield* fixture.session
          .synthetic({ sessionID: fixture.created.id, text: "unrelated", resume: false })
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      // The unproved process-family ticket is still a separate barrier.
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
    }),
  )

  it.live("drain v1 retains unresolved shell ownership even after its leader exits", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const database = yield* Database.Service
      const releasePath = path.join(fixture.tmp.path, "drain.release")
      const startedPath = path.join(fixture.tmp.path, "drain.started")
      const release = Effect.promise(() => Bun.write(releasePath, ""))
      yield* Effect.addFinalizer(() => release)
      const command =
        process.platform === "win32"
          ? "New-Item drain.started -ItemType File | Out-Null; while (!(Test-Path drain.release)) { Start-Sleep -Milliseconds 10 }"
          : "touch drain.started; while [ ! -f drain.release ]; do sleep 0.01; done"
      const child = yield* fixture.shell.create({
        command,
        cwd: fixture.tmp.path,
        metadata: { sessionID: fixture.created.id },
        timeout: 0,
      })
      yield* Effect.promise(() => Bun.file(startedPath).exists()).pipe(
        Effect.repeat({ until: (exists) => exists, schedule: Schedule.spaced("10 millis") }),
        Effect.timeout("5 seconds"),
      )
      const head = yield* database.db
        .select({ seq: EventSequenceTable.seq })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, fixture.created.id))
        .get()
        .pipe(Effect.orDie)
      if (!head) return yield* Effect.die("missing native sequence")
      const request = {
        v: 1 as const,
        sessionID: fixture.created.id,
        requestID: "00000000-0000-4000-8000-000000000030",
        generation: 0,
        sequence: head.seq,
      }
      yield* SessionDrain.begin(request)
      expect(yield* fixture.execution.isActive(fixture.created.id)).toBe(false)
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect(
        yield* fixture.shell
          .create({ command: "exit 0", metadata: { sessionID: fixture.created.id } })
          .pipe(Effect.exit),
      ).toMatchObject({ _tag: "Failure" })
      yield* release
      expect((yield* fixture.shell.wait(child.id)).status).toBe("exited")
      expect(yield* SessionDrain.complete(request).pipe(Effect.exit)).toMatchObject({ _tag: "Failure" })
      expect(yield* fixture.shell.list()).toEqual([])
    }),
  )

  it.live("runs shells concurrently with an active model and waits for each shell's own completion", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const model = yield* fixture.execution.resume(fixture.created.id).pipe(Effect.forkScoped)
      yield* Deferred.await(fixture.control.started).pipe(Effect.timeout("5 seconds"))
      const first = yield* launch(fixture, "first")
      const second = yield* launch(fixture, "second")

      expect(yield* fixture.execution.active).toContain(fixture.created.id)
      expect(first.caller.pollUnsafe()).toBeUndefined()
      expect(second.caller.pollUnsafe()).toBeUndefined()
      expect(yield* fixture.session.messages({ sessionID: fixture.created.id, order: "asc" })).toMatchObject([
        { type: "shell", shellID: first.shellID, status: "running", metadata: { background: true } },
        { type: "shell", shellID: second.shellID, status: "running", metadata: { background: true } },
      ])

      yield* second.release
      yield* Fiber.join(second.caller).pipe(Effect.timeout("5 seconds"))
      expect(first.caller.pollUnsafe()).toBeUndefined()
      expect(yield* fixture.session.inbox(fixture.created.id)).toMatchObject([
        {
          type: "synthetic",
          payload: {
            metadata: { source: "shell", shellID: second.shellID, state: "completed" },
            text: expect.stringContaining("second finished"),
          },
        },
      ])
      yield* first.release
      yield* Fiber.join(first.caller).pipe(Effect.timeout("5 seconds"))
      expect(
        (yield* fixture.session.inbox(fixture.created.id))
          .filter((item) => item.type === "synthetic")
          .map((item) => item.payload.metadata?.shellID),
      ).toEqual([second.shellID, first.shellID])
      expect(yield* fixture.execution.active).toContain(fixture.created.id)
      expect(fixture.control.wakes).toEqual([])
      yield* Deferred.succeed(fixture.control.release, undefined)
      yield* Fiber.join(model).pipe(Effect.timeout("5 seconds"))
    }),
  )

  it.live("routes shell completion to the moved Session while retaining output from its original Location", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const destination = Location.Ref.make({ directory: AbsolutePath.make((yield* tmpdirScoped()).path) })
      const command = yield* launch(fixture, "moved")

      const bus = yield* Bus.Service
      const Done = Bus.ephemeral({ type: "test.shell.move.done", schema: {} })
      const readers = yield* Effect.forEach([fixture.created.location, destination], (ref) =>
        bus.subscribe([SessionEvent.Shell.Ended, SessionEvent.InboxEnqueued, Done]).pipe(
          Stream.takeUntil((event) => event.type === Done.type),
          Stream.runCollect,
          Effect.provideService(Location.Service, location(ref)),
          Effect.forkScoped({ startImmediately: true }),
        ),
      )
      // This fixture's runner does not drain move requests; project the actual move event instead.
      yield* bus.publish(SessionEvent.Moved, {
        sessionID: fixture.created.id,
        location: destination,
        projectID: fixture.created.projectID,
      })
      expect((yield* fixture.session.get(fixture.created.id)).location).toEqual(destination)
      yield* command.release
      yield* Fiber.join(command.caller).pipe(Effect.timeout("5 seconds"))
      yield* bus.publish(Done, {})

      const events = yield* Effect.forEach(readers, (reader) => Fiber.join(reader).pipe(Effect.timeout("5 seconds")))
      expect(events.map((items) => items.map((event) => event.type))).toEqual([
        [Done.type],
        ["session.shell.ended", "session.inbox.enqueued", Done.type],
      ])
      expect(events[1][0]).toMatchObject({
        data: { output: { output: expect.stringContaining("moved finished") } },
      })
      const inbox = yield* fixture.session.inbox(fixture.created.id)
      expect(inbox).toHaveLength(1)
      expect(inbox[0]).toMatchObject({
        type: "synthetic",
        payload: {
          text: expect.stringContaining("moved finished"),
          metadata: { shellID: command.shellID, state: "completed" },
        },
      })
    }),
  )

  it.live("does not suppress a normal prompt wake while a shell is running or wake again when it completes", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const command = yield* launch(fixture, "prompt")

      const prompt = yield* fixture.session.prompt({ sessionID: fixture.created.id, text: "Continue while this runs" })
      yield* Deferred.await(fixture.control.started).pipe(Effect.timeout("5 seconds"))
      expect(fixture.control.wakes).toEqual([fixture.created.id])
      expect(command.caller.pollUnsafe()).toBeUndefined()
      yield* Deferred.succeed(fixture.control.release, undefined)
      yield* fixture.execution.awaitIdle(fixture.created.id)

      yield* command.release
      yield* Fiber.join(command.caller).pipe(Effect.timeout("5 seconds"))
      expect(fixture.control.wakes).toEqual([fixture.created.id])
      expect(yield* fixture.execution.active).toEqual(new Set())
      expect(yield* fixture.session.inbox(fixture.created.id)).toMatchObject([
        { id: prompt.id, type: "user" },
        { type: "synthetic", payload: { metadata: { source: "shell" } } },
      ])
    }),
  )

  for (const exit of [0, 7]) {
    it.live(`records output and admits one completion without waking the model after exit ${exit}`, () =>
      Effect.gen(function* () {
        const fixture = yield* setup
        const command =
          process.platform === "win32"
            ? `Write-Output 'user output'; [Console]::Error.WriteLine('user error'); exit ${exit}`
            : `printf 'user output\\n'; printf 'user error\\n' >&2; exit ${exit}`
        yield* fixture.session.shell({ sessionID: fixture.created.id, command })

        const events = yield* log(fixture.session, fixture.created.id).pipe(Stream.runCollect)
        expect(events.map((event) => event.type)).toEqual([
          "session.created",
          "session.shell.started",
          "session.shell.ended",
          "session.inbox.enqueued",
        ])
        expect(events[1]).toMatchObject({ data: { shell: { metadata: { background: true } } } })
        const messages = yield* fixture.session.messages({ sessionID: fixture.created.id })
        expect(messages).toHaveLength(1)
        expect(messages[0]).toMatchObject({
          type: "shell",
          command,
          status: "exited",
          exit,
          metadata: { background: true },
        })
        const message = messages[0]
        if (message?.type !== "shell") return yield* Effect.die("Missing shell projection")
        expect(message.time.completed).toBeDefined()
        expect(message.output?.output).toContain("user output")
        expect(message.output?.output).toContain("user error")
        expect(message.output?.truncated).toBe(false)

        const inbox = yield* fixture.session.inbox(fixture.created.id)
        expect(inbox).toHaveLength(1)
        expect(inbox[0]).toMatchObject({
          type: "synthetic",
          delivery: "steer",
        })
        const completion = inbox[0]
        if (completion?.type !== "synthetic") return yield* Effect.die("Missing shell completion")
        expect(completion.payload.metadata).toEqual({
          source: "shell",
          shellID: message.shellID,
          state: "completed",
          exit,
          truncated: false,
        })
        expect(completion.payload.description).toBeUndefined()
        expect(completion.payload.text).toContain(command)
        expect(completion.payload.text).toContain("user output")
        expect(completion.payload.text).toContain("user error")
        if (exit === 0) expect(completion.payload.text).not.toContain("Exited with code")
        if (exit !== 0) expect(completion.payload.text).toContain(`Exited with code ${exit}`)
        expect(fixture.control.wakes).toEqual([])
      }),
    )
  }

  for (const outcome of [
    {
      status: "killed",
      state: "cancelled",
      text: "Cancelled",
      output: "Shell command output is no longer available.",
    },
    { status: "timeout", state: "completed", text: "Timed out before completion", output: "timeout started" },
  ]) {
    it.live(`records a ${outcome.status} shell and admits its completion without waking the model`, () =>
      Effect.gen(function* () {
        const fixture = yield* setup
        const command = yield* launch(fixture, outcome.status)
        yield* outcome.status === "killed"
          ? fixture.shell.remove(command.shellID)
          : fixture.shell.timeout(command.shellID, 1)
        yield* Fiber.join(command.caller).pipe(Effect.timeout("5 seconds"))
        expect(yield* fixture.session.messages({ sessionID: fixture.created.id })).toMatchObject([
          {
            type: "shell",
            shellID: command.shellID,
            status: outcome.status,
            time: { completed: expect.anything() },
            output: { output: expect.stringContaining(outcome.output) },
          },
        ])
        const inbox = yield* fixture.session.inbox(fixture.created.id)
        expect(inbox).toHaveLength(1)
        expect(inbox).toMatchObject([
          {
            type: "synthetic",
            payload: {
              text: expect.stringContaining(outcome.text),
              metadata: { source: "shell", shellID: command.shellID, state: outcome.state },
            },
          },
        ])
        expect(inbox[0]).not.toHaveProperty("payload.metadata.exit")
        expect(fixture.control.wakes).toEqual([])
      }),
    )
  }

  it.live("admits a spawn failure without waking the model before failing the caller", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      // Keep the Location initialized, but invalidate the real child's working directory.
      yield* Effect.promise(() => fs.rm(fixture.tmp.path, { recursive: true, force: true }))
      const command = "echo should-not-start"
      const exit = yield* fixture.session.shell({ sessionID: fixture.created.id, command }).pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      expect(yield* fixture.session.messages({ sessionID: fixture.created.id })).toEqual([])
      const inbox = yield* fixture.session.inbox(fixture.created.id)
      expect(inbox).toHaveLength(1)
      expect(inbox[0]).toMatchObject({
        type: "synthetic",
        payload: { text: expect.stringContaining(command), metadata: { source: "shell", state: "error" } },
      })
      expect(fixture.control.wakes).toEqual([])
    }),
  )

  it.live("keeps the shell and completion alive when the waiting caller is interrupted", () =>
    Effect.gen(function* () {
      const fixture = yield* setup
      const command = yield* launch(fixture, "detached")

      yield* Fiber.interrupt(command.caller).pipe(Effect.timeout("5 seconds"))
      const exit = yield* Fiber.await(command.caller)
      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
      expect(yield* fixture.shell.get(command.shellID)).toMatchObject({ status: "running" })
      expect(yield* fixture.session.inbox(fixture.created.id)).toEqual([])

      yield* command.release
      yield* log(fixture.session, fixture.created.id, true).pipe(
        Stream.filter((event) => event.type === "session.inbox.enqueued"),
        Stream.runHead,
        Effect.timeout("5 seconds"),
      )
      expect(yield* fixture.session.messages({ sessionID: fixture.created.id })).toMatchObject([
        {
          type: "shell",
          shellID: command.shellID,
          status: "exited",
          exit: 0,
          output: { output: expect.stringContaining("detached finished") },
        },
      ])
      expect(yield* fixture.session.inbox(fixture.created.id)).toMatchObject([
        {
          type: "synthetic",
          payload: {
            text: expect.stringContaining("detached finished"),
            metadata: { shellID: command.shellID, state: "completed" },
          },
        },
      ])
      expect(fixture.control.wakes).toEqual([])
    }),
  )
})
