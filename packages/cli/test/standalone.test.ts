import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { isolatedEnv } from "./fixture/environment"

test("standalone interrupt cancels one original orphan without running startup recovery or queued input", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-orphan-api-"))
  const env = {
    PATH: process.env.PATH,
    HOME: root,
    USERPROFILE: root,
    TMPDIR: root,
    OPENCODE_TEST_HOME: root,
    OPENCODE_CONFIG_CONTENT: "{}",
    OPENCODE_CONFIG_DIR: path.join(root, "config"),
    OPENCODE_CONFIG_PROJECT_DISABLE: "true",
    OPENCODE_DB: path.join(root, "opencode.db"),
    OPENCODE_DISABLE_FILEWATCHER: "true",
    OPENCODE_DISABLE_MODELS_FETCH: "true",
    XDG_CACHE_HOME: path.join(root, "cache"),
    XDG_CONFIG_HOME: path.join(root, "xdg-config"),
    XDG_DATA_HOME: path.join(root, "data"),
    XDG_STATE_HOME: path.join(root, "state"),
  }
  const api = async (...args: string[]) => {
    const child = Bun.spawn(
      [process.execPath, path.join(import.meta.dir, "../src/index.ts"), "api", ...args, "--standalone"],
      { cwd: path.join(import.meta.dir, ".."), env, stdin: "ignore", stdout: "pipe", stderr: "pipe" },
    )
    const [exit, output, error] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    expect(exit, error).toBe(0)
    return JSON.parse(output)
  }
  try {
    await api("get", "/api/info")
    const dead = Bun.spawn([process.execPath, "-e", ""], { env })
    await dead.exited
    const database = new Database(path.join(root, "opencode.db"))
    try {
      database
        .query("INSERT OR IGNORE INTO project(id,worktree,sandboxes,time_created,time_updated) VALUES(?,?,?,?,?)")
        .run("global", root, "[]", 1, 1)
      for (const id of ["ses_stop_original", "ses_do_not_recover"]) {
        database
          .query(
            "INSERT INTO session_v2(id,project_id,slug,directory,version,time_created,time_updated,time_suspended,claim_pid,resume_attempts) VALUES(?,?,?,?,?,?,?,?,?,?)",
          )
          .run(id, "global", id, root, "fixture", 1, 1, 123, dead.pid, 0)
      }
      database
        .query(
          "INSERT INTO session_inbox(id,session_id,type,payload,delivery,enqueued_seq,time_created) VALUES(?,?,?,?,?,?,?)",
        )
        .run(
          "msg_parked",
          "ses_stop_original",
          "synthetic",
          JSON.stringify({ text: "Parked completion", metadata: { source: "shell", state: "completed" } }),
          "steer",
          0,
          1,
        )
      const before = database.query("SELECT * FROM session_inbox").all()
      expect(await api("post", "/api/session/ses_stop_original/interrupt")).toEqual({ interrupted: true })
      expect(await api("post", "/api/session/ses_stop_original/interrupt")).toEqual({ interrupted: false })
      expect(
        database
          .query(
            "SELECT claim_pid,time_suspended,resume_attempts,idle_outcome FROM session_v2 WHERE id='ses_stop_original'",
          )
          .get(),
      ).toEqual({ claim_pid: null, time_suspended: null, resume_attempts: 0, idle_outcome: "interrupted" })
      expect(
        database
          .query("SELECT claim_pid,time_suspended,resume_attempts FROM session_v2 WHERE id='ses_do_not_recover'")
          .get(),
      ).toEqual({ claim_pid: dead.pid, time_suspended: 123, resume_attempts: 0 })
      expect(database.query("SELECT * FROM session_inbox").all()).toEqual(before)
      expect(database.query("SELECT session_id,type FROM session_message").all()).toEqual([
        { session_id: "ses_stop_original", type: "idle" },
      ])
    } finally {
      database.close()
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true })
  }
}, 90_000)

test("standalone server exits when its owner is killed", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-cli-standalone-"))
  const owner = Bun.spawn([process.execPath, path.join(import.meta.dir, "fixture/standalone-owner.ts")], {
    cwd: path.join(import.meta.dir, ".."),
    env: isolatedEnv(root, { OPENCODE_SERVER_USERNAME: "custom" }),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  })
  const line = await Promise.race([
    readLine(owner.stdout, "STANDALONE_READY "),
    Bun.sleep(10_000).then(() => undefined),
  ])
  const [, rawPID, url, status] = line?.split(" ") ?? []
  const pid = Number(rawPID)

  try {
    expect(pid).toBeGreaterThan(0)
    expect(url).toStartWith("http://127.0.0.1:")
    expect(status).toBe("200")
    expect(running(pid)).toBe(true)

    owner.kill("SIGKILL")
    await owner.exited

    expect(await waitForExit(pid)).toBe(true)
  } finally {
    owner.kill("SIGKILL")
    await owner.exited
    if (running(pid)) process.kill(pid, "SIGKILL")
    await fs.rm(root, { recursive: true, force: true })
  }
})

async function readLine(stream: ReadableStream<Uint8Array>, prefix: string) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  while (true) {
    const result = await reader.read()
    if (result.done) break
    chunks.push(decoder.decode(result.value, { stream: true }))
    const output = chunks.join("")
    const line = output.split("\n").find((line) => line.startsWith(prefix))
    if (line) {
      reader.releaseLock()
      return line
    }
  }
  reader.releaseLock()
  return (chunks.join("") + decoder.decode()).split("\n").find((line) => line.startsWith(prefix))
}

async function waitForExit(pid: number, attempts = 100): Promise<boolean> {
  if (!running(pid)) return true
  if (attempts === 0) return false
  await Bun.sleep(50)
  return waitForExit(pid, attempts - 1)
}

function running(pid: number) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}
