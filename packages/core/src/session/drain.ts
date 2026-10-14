export * as SessionDrain from "./drain.js"

import { randomUUID } from "node:crypto"
import { isDeepStrictEqual } from "node:util"
import { and, eq, gte, inArray, isNotNull, lt, or, sql } from "drizzle-orm"
import { Effect, Exit, Schema } from "effect"
import { Session } from "@opencode/schema/session"
import { NonNegativeInt } from "@opencode/schema/schema"
import { supportsTuningPragmas } from "#sqlite"
import { Database } from "../database/database.js"
import { EventSequenceTable } from "../event/sql.js"
import { KVTable } from "../kv/sql.js"
import { SessionInboxTable, SessionTable } from "./sql.js"

type DB = Database.Interface["db"]
const prefix = "session.drain/"
const workPrefix = "session.work/"
const Sequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1))
export const Request = Schema.Struct({
  v: Schema.Literal(1),
  sessionID: Session.ID,
  requestID: Schema.String.check(Schema.isPattern(/^[a-f0-9-]{36}$/)),
  generation: NonNegativeInt,
  sequence: Sequence,
})
export type Request = typeof Request.Type
const Receipt = Schema.Struct({
  request: Request,
  generation: NonNegativeInt,
  phase: Schema.Literals(["holding", "quiescent"]),
  sequence: Sequence,
})
export type Receipt = typeof Receipt.Type
export class Held extends Schema.TaggedError<Held>()("SessionDrain.Held", { sessionID: Session.ID }) {}
export class Conflict extends Schema.TaggedError<Conflict>()("SessionDrain.Conflict", { sessionID: Session.ID }) {}
export class Busy extends Schema.TaggedError<Busy>()("SessionDrain.Busy", { sessionID: Session.ID }) {}
export class Unavailable extends Schema.TaggedError<Unavailable>()("SessionDrain.Unavailable", {
  reason: Schema.Literals(["durability-unsupported", "durability-unavailable"]),
}) {}

// The ordinary native database uses NORMAL. A fence acknowledgement must not
// have that power-loss rollback window; keep this connection at FULL thereafter.
const requireDurability = Effect.fn("SessionDrain.requireDurability")(function* (db: DB) {
  if (!supportsTuningPragmas) return yield* Effect.die(new Unavailable({ reason: "durability-unsupported" }))
  const before = yield* db.get<{ synchronous: number }>(sql`PRAGMA synchronous`).pipe(Effect.orDie)
  if (before?.synchronous !== 2 && before?.synchronous !== 3)
    yield* db.run(sql`PRAGMA synchronous = FULL`).pipe(Effect.orDie)
  const after = yield* db.get<{ synchronous: number }>(sql`PRAGMA synchronous`).pipe(Effect.orDie)
  if (after?.synchronous !== 2 && after?.synchronous !== 3)
    return yield* Effect.die(new Unavailable({ reason: "durability-unavailable" }))
})

/** Read inside the same transaction that admits work. Ancestor holds also cover
 * children created before the hold; no client-supplied descendant list is trusted. */
export const isHeld = Effect.fn("SessionDrain.isHeld")(function* (db: DB, sessionID: Session.ID) {
  const held = yield* db
    .get<{ id: string }>(
      sql`
    WITH RECURSIVE ancestors(id, parent_id) AS (
      SELECT ${sessionID}, (SELECT parent_id FROM session_v2 WHERE id = ${sessionID})
      UNION SELECT a.parent_id, (SELECT s.parent_id FROM session_v2 s WHERE s.id = a.parent_id)
      FROM ancestors a WHERE a.parent_id IS NOT NULL
    ) SELECT a.id FROM ancestors a JOIN kv ON kv.key = ${prefix} || a.id LIMIT 1
  `,
    )
    .pipe(Effect.orDie)
  return held !== undefined
})

export const assertOpen = (db: DB, sessionID: Session.ID) =>
  isHeld(db, sessionID).pipe(Effect.flatMap((held) => (held ? Effect.die(new Held({ sessionID })) : Effect.void)))

/** The ticket precedes actual dispatch. A crash or uncertain tool exit leaves
 * evidence of outstanding work; PID death and idle status never erase it. */
export const enter = Effect.fn("SessionDrain.enter")(function* (
  db: DB,
  sessionID: Session.ID,
  kind: "execution" | "tool" | "shell" | "job",
) {
  const key = `${workPrefix}${sessionID}/${randomUUID()}`
  yield* db
    .transaction(() =>
      Effect.gen(function* () {
        yield* assertOpen(db, sessionID)
        yield* db
          .insert(KVTable)
          .values({ key, value: { v: 1, sessionID, generation: 0, kind, pid: process.pid } })
          .run()
      }),
    )
    .pipe(Effect.orDie)
  return {
    // Failed/cancelled work remains unresolved: its status or transport error
    // cannot establish whether external effects stopped. Never expire tickets.
    finish: (exit: Exit.Exit<unknown, unknown>) =>
      Exit.isSuccess(exit)
        ? db.delete(KVTable).where(eq(KVTable.key, key)).run().pipe(Effect.orDie, Effect.asVoid)
        : Effect.void,
  }
})

export const work = <A, E, R>(
  db: DB,
  sessionID: Session.ID,
  kind: "tool" | "shell" | "job",
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const ticket = yield* enter(db, sessionID, kind)
      return yield* restore(effect).pipe(Effect.onExit((exit: Exit.Exit<A, E>) => ticket.finish(exit)))
    }),
  )

const sequence = (db: DB, sessionID: Session.ID) =>
  db
    .select({ seq: EventSequenceTable.seq })
    .from(EventSequenceTable)
    .where(eq(EventSequenceTable.aggregate_id, sessionID))
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => row?.seq ?? -1),
    )

const load = (db: DB, request: Request) =>
  db
    .select({ value: KVTable.value })
    .from(KVTable)
    .where(eq(KVTable.key, prefix + request.sessionID))
    .get()
    .pipe(
      Effect.orDie,
      Effect.flatMap((row) => {
        if (!row) return Effect.succeed(undefined)
        const receipt = Schema.decodeUnknownSync(Receipt)(row.value)
        if (
          receipt.generation !== 1 ||
          receipt.request.generation !== 0 ||
          receipt.sequence < receipt.request.sequence ||
          (receipt.phase === "holding" && receipt.sequence !== receipt.request.sequence)
        )
          return Effect.die(new Conflict({ sessionID: request.sessionID }))
        return Effect.succeed(receipt)
      }),
    )

/** Native v1 source API only; no HTTP route advertises this yet. This is a
 * graceful drain: already admitted owned work must settle, never be inferred
 * stopped from a cancellation request. Readmission is intentionally absent. */
export const begin = Effect.fn("SessionDrain.begin")(function* (input: Request) {
  const request = Schema.decodeUnknownSync(Request)({ ...input })
  const db = (yield* Database.Service).db
  yield* requireDurability(db)
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const saved = yield* load(db, request)
        if (saved) {
          if (!isDeepStrictEqual(saved.request, request))
            return yield* Effect.die(new Conflict({ sessionID: request.sessionID }))
          return saved
        }
        const root = yield* db
          .select({ parent: SessionTable.parent_id })
          .from(SessionTable)
          .where(eq(SessionTable.id, request.sessionID))
          .get()
        if (
          !root ||
          root.parent !== null ||
          request.generation !== 0 ||
          (yield* sequence(db, request.sessionID)) !== request.sequence
        )
          return yield* Effect.die(new Conflict({ sessionID: request.sessionID }))
        yield* assertOpen(db, request.sessionID)
        const receipt: Receipt = { request, generation: 1, phase: "holding", sequence: request.sequence }
        yield* db
          .insert(KVTable)
          .values({ key: prefix + request.sessionID, value: receipt })
          .run()
        return receipt
      }),
    )
    .pipe(Effect.orDie)
})

export const complete = Effect.fn("SessionDrain.complete")(function* (input: Request) {
  const request = Schema.decodeUnknownSync(Request)({ ...input })
  const db = (yield* Database.Service).db
  yield* requireDurability(db)
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const saved = yield* load(db, request)
        if (!saved || !isDeepStrictEqual(saved.request, request))
          return yield* Effect.die(new Conflict({ sessionID: request.sessionID }))
        const rows = yield* db.all<{ id: Session.ID }>(sql`
      WITH RECURSIVE family(id) AS (
        SELECT id FROM session_v2 WHERE id = ${request.sessionID}
        UNION SELECT s.id FROM session_v2 s JOIN family f ON s.parent_id = f.id
      ) SELECT id FROM family LIMIT 257
    `)
        if (rows.length === 0 || rows.length > 256) return yield* Effect.die(new Busy({ sessionID: request.sessionID }))
        const ids = rows.map((row) => row.id)
        const claim = yield* db
          .select({ id: SessionTable.id })
          .from(SessionTable)
          .where(and(inArray(SessionTable.id, ids), isNotNull(SessionTable.time_suspended)))
          .limit(1)
          .get()
        const queued = yield* db
          .select({ id: SessionInboxTable.id })
          .from(SessionInboxTable)
          .where(inArray(SessionInboxTable.session_id, ids))
          .limit(1)
          .get()
        const work = yield* db
          .select({ key: KVTable.key })
          .from(KVTable)
          .where(
            or(
              ...ids.map((id) =>
                and(gte(KVTable.key, `${workPrefix}${id}/`), lt(KVTable.key, `${workPrefix}${id}/\uffff`)),
              ),
            ),
          )
          .limit(1)
          .get()
        // Older durable background markers may have no v1 work ticket. Missing
        // process ownership is not evidence that such a shell/child stopped.
        const background = yield* db
          .select({ key: KVTable.key })
          .from(KVTable)
          .where(
            and(
              gte(KVTable.key, "job.background/"),
              lt(KVTable.key, "job.background/\uffff"),
              sql`(json_extract(${KVTable.value}, '$.drainVersion') IS NOT 1 OR coalesce(json_extract(${KVTable.value}, '$.status'), 'unknown') != 'completed')`,
              or(
                inArray(sql`json_extract(${KVTable.value}, '$.recovery.sessionID')`, ids),
                inArray(sql`json_extract(${KVTable.value}, '$.recovery.parentSessionID')`, ids),
                inArray(sql`json_extract(${KVTable.value}, '$.recovery.childSessionID')`, ids),
              ),
            ),
          )
          .limit(1)
          .get()
      if (claim || queued || work || background) return yield* Effect.die(new Busy({ sessionID: request.sessionID }))
        const current = yield* sequence(db, request.sessionID)
        if (saved.phase === "quiescent" && saved.sequence !== current)
          return yield* Effect.die(new Conflict({ sessionID: request.sessionID }))
        if (saved.phase === "quiescent") return saved
        const receipt: Receipt = { ...saved, phase: "quiescent", sequence: current }
        yield* db
          .update(KVTable)
          .set({ value: receipt })
          .where(eq(KVTable.key, prefix + request.sessionID))
          .run()
        return receipt
      }),
    )
    .pipe(Effect.orDie)
})
