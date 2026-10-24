import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { LLM, LLMEvent } from "../../src/index.js"
import { LLMClient } from "../../src/route.js"
import { Gemini } from "../../src/protocols/gemini.js"
import { routes } from "../../src/providers/google-vertex.js"
import { it } from "../lib/effect.js"
import { fixedResponse } from "../lib/http.js"
import { sseEvents } from "../lib/sse.js"

const request = LLM.request({ model: Gemini.route.model({ id: "gemini-2.5-flash" }), prompt: "Search." })
const groundingMetadata = {
  webSearchQueries: ["example query"],
  searchEntryPoint: { renderedContent: "<div>Search suggestions</div>" },
  groundingChunks: [
    { web: { uri: "https://example.test/source", title: "Example source" } },
    { web: { uri: "https://example.test/source", title: "Example source" } },
  ],
  groundingSupports: [
    {
      segment: { startIndex: 0, endIndex: 6, text: "Answer" },
      groundingChunkIndices: [0, 1],
      confidenceScores: [0.9, 0.8],
    },
  ],
}

describe("Gemini grounding retention", () => {
  it.effect("shares retention with Vertex and keeps signed text replay intact", () =>
    Effect.gen(function* () {
      const model = routes[0].with({ endpoint: { baseURL: "https://vertex.test" } }).model({ id: "gemini-3-flash" })
      const response = yield* LLMClient.generate(LLM.request({ model, prompt: "Search." })).pipe(
        Effect.provide(
          fixedResponse(
            sseEvents(
              { candidates: [{ content: { parts: [{ text: "Answer" }] } }] },
              {
                candidates: [
                  {
                    content: { parts: [{ text: "", thoughtSignature: "signed-answer" }] },
                    groundingMetadata,
                    finishReason: "STOP",
                  },
                ],
              },
            ),
          ),
        ),
      )
      expect(response.events.find(LLMEvent.is.textEnd)?.providerMetadata).toEqual({
        vertex: {
          groundingMetadata,
          thoughtSignature: "signed-answer",
        },
      })
      const body = yield* Gemini.protocol.body.from(LLM.request({ model, messages: [response.message] }))
      expect(body.contents).toEqual([{ role: "model", parts: [{ text: "Answer", thoughtSignature: "signed-answer" }] }])
      const next = yield* LLMClient.generate(request).pipe(
        Effect.provide(
          fixedResponse(
            sseEvents({ candidates: [{ content: { parts: [{ text: "Unrelated" }] }, finishReason: "STOP" }] }),
          ),
        ),
      )
      expect(next.events.find(LLMEvent.is.finish)?.providerMetadata).toBeUndefined()
    }),
  )

  it.effect("retains final metadata-only SSE on actual events and collected text", () =>
    Effect.gen(function* () {
      const response = yield* LLMClient.generate(request).pipe(
        Effect.provide(
          fixedResponse(
            sseEvents(
              { candidates: [{ content: { parts: [{ text: "Answer" }] } }] },
              { candidates: [{ groundingMetadata, finishReason: "STOP" }] },
            ),
          ),
        ),
      )
      expect(response.text).toBe("Answer")
      expect(response.events.find(LLMEvent.is.textEnd)?.providerMetadata).toEqual({ google: { groundingMetadata } })
      expect(response.events.find(LLMEvent.is.finish)?.providerMetadata).toEqual({ google: { groundingMetadata } })
      expect(response.message.content).toMatchObject([
        { type: "text", text: "Answer", providerMetadata: { google: { groundingMetadata } } },
      ])
      expect(response.events.filter(LLMEvent.is.finish)).toHaveLength(1)
      expect(response.events.filter(LLMEvent.is.toolCall)).toHaveLength(0)
      expect(response.events.filter(LLMEvent.is.toolResult)).toHaveLength(0)
    }),
  )

  it.effect("retains grounding and citations at finish without inventing text or fetched page content", () =>
    Effect.gen(function* () {
      const citationMetadata = { citationSources: [{ uri: "https://example.test/source", startIndex: 0, endIndex: 6 }] }
      const response = yield* LLMClient.generate(request).pipe(
        Effect.provide(
          fixedResponse(sseEvents({ candidates: [{ groundingMetadata, citationMetadata, finishReason: "STOP" }] })),
        ),
      )
      expect(response.text).toBe("")
      expect(response.message.content).toEqual([])
      expect(response.events.find(LLMEvent.is.finish)?.providerMetadata).toEqual({
        google: { groundingMetadata, citationMetadata },
      })
      expect(response.events.filter(LLMEvent.is.finish)).toHaveLength(1)
    }),
  )

  it.effect("selects a nonempty candidate and follows its index without cross-merging", () =>
    Effect.gen(function* () {
      const response = yield* LLMClient.generate(request).pipe(
        Effect.provide(
          fixedResponse(
            sseEvents(
              {
                candidates: [
                  { index: 0, content: { parts: [] } },
                  { index: 1, content: { parts: [{ text: "Answer" }] } },
                ],
              },
              {
                candidates: [
                  {
                    index: 0,
                    content: { parts: [{ text: "Wrong" }] },
                    groundingMetadata: { webSearchQueries: ["wrong"] },
                    finishReason: "STOP",
                  },
                ],
              },
              {
                candidates: [
                  { index: 1, groundingMetadata, finishReason: "STOP" },
                  { index: 0, finishReason: "MALFORMED_FUNCTION_CALL" },
                ],
              },
            ),
          ),
        ),
      )
      expect(response.text).toBe("Answer")
      expect(response.events.find(LLMEvent.is.finish)?.providerMetadata).toEqual({ google: { groundingMetadata } })
    }),
  )

  it.effect("preserves queries across partial metadata and repeated chunks without rewriting support indices", () =>
    Effect.gen(function* () {
      const response = yield* LLMClient.generate(request).pipe(
        Effect.provide(
          fixedResponse(
            sseEvents(
              {
                candidates: [
                  {
                    content: { parts: [{ text: "Answer" }] },
                    groundingMetadata: { webSearchQueries: groundingMetadata.webSearchQueries },
                  },
                ],
              },
              { candidates: [{ groundingMetadata: { groundingChunks: groundingMetadata.groundingChunks } }] },
              {
                candidates: [
                  {
                    groundingMetadata: {
                      groundingChunks: groundingMetadata.groundingChunks,
                      groundingSupports: groundingMetadata.groundingSupports,
                    },
                    finishReason: "STOP",
                  },
                ],
              },
            ),
          ),
        ),
      )
      expect(response.events.find(LLMEvent.is.finish)?.providerMetadata).toEqual({
        google: {
          groundingMetadata: {
            webSearchQueries: groundingMetadata.webSearchQueries,
            groundingChunks: groundingMetadata.groundingChunks,
            groundingSupports: groundingMetadata.groundingSupports,
          },
        },
      })
    }),
  )

  it.effect("retains missing URLs without inventing sources", () =>
    Effect.gen(function* () {
      const metadata = {
        groundingChunks: [{ web: { title: "Unavailable source" } }],
        webSearchQueries: [],
      }
      const response = yield* LLMClient.generate(request).pipe(
        Effect.provide(
          fixedResponse(
            sseEvents(
              { candidates: [{ content: { parts: [{ text: "No source available." }] }, groundingMetadata: metadata }] },
              { candidates: [{ groundingMetadata: null, citationMetadata: null, finishReason: "STOP" }] },
            ),
          ),
        ),
      )
      expect(response.events.find(LLMEvent.is.finish)?.providerMetadata).toEqual({
        google: { groundingMetadata: metadata },
      })
      expect(response.text).toBe("No source available.")
    }),
  )
})
