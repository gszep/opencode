import { expect } from "bun:test"
import { Effect, Layer } from "effect"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionTransfer } from "@opencode/core/session/transfer"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

it.live("maps an unsettled export to HTTP 409 without a partial transcript", () =>
  Effect.gen(function* () {
    const sessionID = Session.ID.create()
    const messageID = SessionMessage.ID.create()
    // Core tests exercise real projection/settlement. This replacement isolates
    // the public HTTP error mapping without launching a model or a live server.
    const handler = yield* ServerFetch.make(
      {
        app: { version: "test" },
        database: { path: ":memory:" },
        config: { project: false },
        models: { fetch: false },
        fs: { filewatcher: false },
      },
      {
        overrides: [
          SessionTransfer.node.replace(
            Layer.succeed(
              SessionTransfer.Service,
              SessionTransfer.Service.of({
                export: () => Effect.fail(new SessionTransfer.UnsettledError({ sessionID, messageID })),
                import: () => Effect.die("Unused import in export-error fixture"),
              }),
            ),
          ),
        ],
      },
    )
    yield* Effect.promise(async () => {
      for (const query of ["", "?sanitize=true"]) {
        const response = await handler(
          new Request(`http://opencode.local/api/experimental/session/${sessionID}/export${query}`),
        )
        expect(response.status).toBe(409)
        const body = await response.json()
        expect(body).toMatchObject({
          _tag: "ConflictError",
          resource: sessionID,
          message: "Session contains unsettled messages; retry export after they complete",
        })
        expect(body).not.toHaveProperty("data")
      }
    })
  }).pipe(Effect.scoped),
)
