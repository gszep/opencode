import { describe, expect, test } from "bun:test"
import { LanguageModel, LLMClient, LLMEvent } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols"
import { Bus } from "@opencode/core/bus"
import { Config } from "@opencode/core/config"
import { ConfigCompactionPlugin } from "@opencode/core/config/plugin/compaction"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { llmClient } from "@opencode/core/effect/app-node-platform"
import { SessionCompaction } from "@opencode/core/session/compaction"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { Session } from "@opencode/core/session"
import { Agent } from "@opencode/core/agent"
import { Location } from "@opencode/core/location"
import { Project } from "@opencode/core/project"
import { AbsolutePath } from "@opencode/core/schema"
import { ConfigCompaction } from "@opencode/schema/config/compaction"
import { Document, Event, Info } from "@opencode/schema/config"
import { Money } from "@opencode/schema/money"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { DateTime, Effect, Fiber, Layer, Option, Schema, Stream } from "effect"
import { testEffect } from "../lib/effect"
import { host } from "../plugin/host"

const model = LanguageModel.make({
  id: "test-model",
  provider: "test-provider",
  route: OpenAIChat.route,
})
const limit = { context: 100_000, output: 1_000 }
const resolved = SessionRunnerModel.resolved(model, {
  capabilities: { tools: true, input: ["text"], output: ["text"] },
  cost: [],
  limit,
})
const config = Config.testLayer()
const it = testEffect(
  Layer.merge(
    config,
    AppNodeBuilder.build(LayerNode.group([SessionCompaction.node, SessionModelRequest.node, Config.node, Bus.node]), [
      llmClient.replace(
        Layer.mock(LLMClient.Service)({
          stream: () => Stream.make(LLMEvent.textDelta({ id: "summary", text: "## Objective\n- summary" })),
        }),
      ),
      Config.node.replace(config),
    ]),
  ),
)
describe("ConfigCompactionPlugin.Plugin", () => {
  test("input cap is optional and accepts only positive integers", () => {
    const decode = Schema.decodeUnknownOption(ConfigCompaction.Info)
    expect(Schema.decodeUnknownSync(ConfigCompaction.Info)({})).toEqual({})
    expect(Schema.decodeUnknownSync(ConfigCompaction.Info)({ input_cap: 128_000 })).toEqual({ input_cap: 128_000 })
    for (const input_cap of [0, -1, 127_999.5]) expect(Option.isNone(decode({ input_cap }))).toBe(true)
  })

  it.live("reloads and removes the optional input cap while inheriting configured keep 15000", () =>
    Effect.gen(function* () {
      const compaction = yield* SessionCompaction.Service
      const modelRequests = yield* SessionModelRequest.Service
      const config = yield* Config.Test
      const bus = yield* Bus.Service
      const selected = { ...resolved, limit: { context: 1_000_000, input: 900_000, output: 32_000 } }
      const sized = (tokens: number) => {
        const current = input(tokens)
        return { ...current, resolved: selected, context: { ...current.context, model: selected } }
      }
      const keep = new Document({
        type: "document",
        info: new Info({
          compaction: new ConfigCompaction.Info({ keep: new ConfigCompaction.Keep({ tokens: 15_000 }) }),
        }),
      })
      const setting = (value: ConfigCompaction.Info) =>
        new Document({ type: "document", info: new Info({ compaction: value }) })
      expect(compaction.required(sized(128_000))).toBe(false)
      yield* config.setEntries([keep, setting(new ConfigCompaction.Info({ input_cap: 128_000 }))])
      yield* ConfigCompactionPlugin.Plugin.effect(host({ event: { subscribe: () => bus.subscribe(Event.Updated) } }))
      expect(compaction.required(sized(96_000))).toBe(false)
      expect(compaction.required(sized(127_999))).toBe(false)
      expect(compaction.required(sized(128_000))).toBe(true)

      const messages = [
        ["Older prefix", "o".repeat(80_000)],
        ["Retained middle", "m".repeat(24_000)],
        ["Newest exchange", "n".repeat(28_000)],
      ].map(([label, text], index) =>
        SessionMessage.User.make({
          id: SessionMessage.ID.create(),
          type: "user",
          text: `${label}\n${text}`,
          time: { created: DateTime.makeUnsafe(index) },
        }),
      )
      const started = yield* bus
        .subscribe(SessionEvent.Compaction.Started)
        .pipe(Stream.runHead, Effect.forkScoped({ startImmediately: true }))
      expect(
        yield* compaction.compactManual({
          session,
          resolveContext: () => Effect.succeed({ ...sized(128_000).context, messages, instructionUpdate: "" }),
          prepare: modelRequests.compaction,
          messages,
          inputID: SessionMessage.ID.create(),
        }),
      ).toEqual({ status: "completed" })
      const recent = Option.getOrThrow(yield* Fiber.join(started)).data.recent
      expect(recent).toContain("Retained middle")
      expect(recent).toContain("Newest exchange")
      expect(recent).not.toContain("Older prefix")

      for (const [settings, probe, expected] of [
        [new ConfigCompaction.Info({ input_cap: 256_000 }), 128_000, false],
        [new ConfigCompaction.Info({ input_cap: 128_000 }), 128_000, true],
        [new ConfigCompaction.Info({ input_cap: 128_000, auto: false }), 128_000, false],
        [new ConfigCompaction.Info({ input_cap: 128_000, auto: true }), 128_000, true],
        [new ConfigCompaction.Info({}), 128_000, false],
      ] as const) {
        yield* config.setEntries([keep, setting(settings)])
        yield* bus.publish(Event.Updated, {})
        for (let attempt = 0; attempt < 200 && compaction.required(sized(probe)) !== expected; attempt++)
          yield* Effect.sleep("10 millis")
        expect(compaction.required(sized(probe))).toBe(expected)
        if (settings.input_cap === 256_000) expect(compaction.required(sized(256_000))).toBe(true)
      }
      expect(compaction.required(sized(256_000))).toBe(false)
      expect(compaction.required(sized(880_000))).toBe(true)
      expect(selected.limit).toEqual({ context: 1_000_000, input: 900_000, output: 32_000 })
    }),
  )

  it.live("merges settings and reloads changed config", () =>
    Effect.gen(function* () {
      const compaction = yield* SessionCompaction.Service
      const modelRequests = yield* SessionModelRequest.Service
      const config = yield* Config.Test
      const bus = yield* Bus.Service
      yield* config.setEntries([
        new Document({
          type: "document",
          info: new Info({ compaction: new ConfigCompaction.Info({ auto: false, buffer: 20_000 }) }),
        }),
        new Document({
          type: "document",
          info: new Info({
            compaction: new ConfigCompaction.Info({
              buffer: 10_000,
              keep: new ConfigCompaction.Keep({ tokens: 0 }),
            }),
          }),
        }),
      ])
      yield* ConfigCompactionPlugin.Plugin.effect(host({ event: { subscribe: () => bus.subscribe(Event.Updated) } }))

      expect(compaction.required(nearInput)).toBe(false)
      const started = yield* bus
        .subscribe(SessionEvent.Compaction.Started)
        .pipe(Stream.runHead, Effect.forkScoped({ startImmediately: true }))
      const messages = [
        SessionMessage.User.make({
          id: SessionMessage.ID.create(),
          type: "user",
          text: "Older context",
          time: { created: DateTime.makeUnsafe(0) },
        }),
        SessionMessage.User.make({
          id: SessionMessage.ID.create(),
          type: "user",
          text: "Recent context",
          time: { created: DateTime.makeUnsafe(1) },
        }),
      ]
      expect(
        yield* compaction.compactManual({
          session,
          resolveContext: () => Effect.succeed({ ...nearInput.context, messages, instructionUpdate: "" }),
          prepare: modelRequests.compaction,
          messages,
          inputID: SessionMessage.ID.make("msg_compaction_manual"),
        }),
      ).toEqual({ status: "completed" })
      expect(Option.getOrThrow(yield* Fiber.join(started)).data.recent).toContain("Recent context")

      yield* config.setEntries([
        new Document({
          type: "document",
          info: new Info({ compaction: new ConfigCompaction.Info({ auto: true, buffer: 20_000 }) }),
        }),
        new Document({
          type: "document",
          info: new Info({ compaction: new ConfigCompaction.Info({ buffer: 10_000 }) }),
        }),
      ])
      yield* bus.publish(Event.Updated, {})
      yield* Effect.gen(function* () {
        for (let attempt = 0; attempt < 200; attempt++) {
          if (compaction.required(nearInput)) return
          yield* Effect.sleep("10 millis")
        }
        yield* Effect.die(new Error("Timed out waiting for compaction config reload"))
      })
      expect(compaction.required(bufferedInput)).toBe(false)

      yield* config.setEntries([
        new Document({
          type: "document",
          info: new Info({ compaction: new ConfigCompaction.Info({ auto: true, buffer: 20_000 }) }),
        }),
      ])
      yield* bus.publish(Event.Updated, {})
      for (let attempt = 0; attempt < 200; attempt++) {
        if (compaction.required(bufferedInput)) return
        yield* Effect.sleep("10 millis")
      }
      yield* Effect.die(new Error("Timed out waiting for compaction config reload"))
    }),
  )
})

const session = Session.Info.make({
  id: Session.ID.make("ses_compaction_config"),
  projectID: Project.ID.global,
  cost: Money.USD.zero,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  time: { created: DateTime.makeUnsafe(0), updated: DateTime.makeUnsafe(0) },
  location: Location.Ref.make({ directory: AbsolutePath.make("/tmp") }),
})
const input = (tokens: number) => {
  const messages = [
    Schema.decodeUnknownSync(SessionMessage.Assistant)({
      id: SessionMessage.ID.make("msg_compaction_config"),
      type: "assistant",
      agent: Agent.defaultID,
      model: { id: "test-model", providerID: "test-provider" },
      content: [],
      tokens: { input: tokens, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: 0, completed: 0 },
    }),
  ]
  return {
    session,
    resolved,
    messages,
    context: {
      session,
      model: resolved,
      messages,
      agent: { id: Agent.defaultID, info: Agent.Info.default(Agent.defaultID) },
      initial: "",
      tools: { definitions: [], execute: () => Effect.die("unused") },
    },
  }
}
const bufferedInput = input(85_000)
const nearInput = input(95_000)
