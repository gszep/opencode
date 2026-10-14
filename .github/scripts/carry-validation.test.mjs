import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdtempSync, chmodSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync, execFileSync } from 'node:child_process';

const workflow = readFileSync(new URL('../workflows/carry-validation.yml', import.meta.url), 'utf8');
function runBlock(name) {
  const step = workflow.split(`      - name: ${name}\n`)[1]?.split('\n      - name: ')[0];
  assert.ok(step, `missing step ${name}`);
  const body = step.split('        run: |\n')[1];
  assert.ok(body, `missing literal script ${name}`);
  return body.split('\n').map(line => line.replace(/^ {10}/, '')).join('\n');
}
const reviewed = name => {
  const value = workflow.match(new RegExp(`^      ${name}: "?([a-z0-9]+)"?$`, 'm'))?.[1];
  assert.ok(value, `missing ${name}`);
  return value;
};
const sha = reviewed('REVIEWED_HEAD'), base = reviewed('REVIEWED_BASE'), pr = reviewed('REVIEWED_PR');
const controller = 'gszep/opencode/.github/workflows/carry-validation.yml@refs/heads/dev';
const preflight = runBlock('Validate reviewed target before executing repository code');
const source = runBlock('Verify source and bounded validation contract');
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'carry-validation-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

test('controller has one read-only manually dispatched validation lane', () => {
  assert.match(workflow, /^"on":\n  workflow_dispatch:/m);
  assert.doesNotMatch(workflow, /^  (?:push|pull_request|pull_request_target|schedule):/m);
  assert.equal(workflow.match(/^permissions:[\s\S]*?(?=\n\n)/m)?.[0], 'permissions:\n  contents: read');
  assert.equal((workflow.match(/permissions:/g) ?? []).length, 1);
  assert.match(workflow, /group: gszep-opencode-carry-validation\n  cancel-in-progress: false/);
  assert.match(workflow, /runs-on: ubuntu-24\.04/);
  assert.match(workflow, /persist-credentials: false/);
  assert.doesNotMatch(workflow, /secrets\.|GH_TOKEN|GITHUB_TOKEN|actions\/(?:cache|upload-artifact)/);
  const actions = [...workflow.matchAll(/^        uses: (\S+)$/gm)].map(match => match[1]);
  assert.equal(actions.length, 3);
  for (const action of actions) assert.match(action, /^(actions\/(checkout|setup-node)|oven-sh\/setup-bun)@[a-f0-9]{40}$/);
  assert.equal(spawnSync('bash', ['-n'], { input: preflight }).status, 0);
  assert.equal(spawnSync('bash', ['-n'], { input: source }).status, 0);
});

test('preflight rejects wrong controller, reviewed target, repository and base before source execution', t => {
  const directory = fixture(t);
  writeFileSync(join(directory, 'curl'), '#!/bin/sh\ncat "$FIXTURE_JSON"\n');
  chmodSync(join(directory, 'curl'), 0o700);
  const body = { number: Number(pr), state: 'open', head: { sha, repo: { full_name: 'gszep/opencode' } },
    base: { ref: 'carry/2.0.15-chi.1', sha: base, repo: { full_name: 'gszep/opencode' } } };
  const env = { ...process.env, PATH: directory + ':' + process.env.PATH, RUNNER_TEMP: directory,
    FIXTURE_JSON: join(directory, 'response.json'), TARGET_PR: pr, TARGET_SHA: sha,
    REVIEWED_PR: pr, REVIEWED_HEAD: sha, REVIEWED_BASE: base,
    CONTROLLER_REF: 'refs/heads/dev', CONTROLLER_WORKFLOW_REF: controller };
  const cases = [
    ['reviewed', body, {}, true],
    ['wrong controller ref', body, { CONTROLLER_REF: 'refs/heads/unreviewed' }, false],
    ['wrong controller workflow', body, { CONTROLLER_WORKFLOW_REF: controller.replace('dev', 'unreviewed') }, false],
    ['different same-repo PR', { ...body, number: Number(pr) + 1 }, { TARGET_PR: String(Number(pr) + 1) }, false],
    ['different same-repo head', { ...body, head: { ...body.head, sha: '0'.repeat(40) } }, { TARGET_SHA: '0'.repeat(40) }, false],
    ['bad PR syntax', body, { TARGET_PR: '3;false' }, false],
    ['bad SHA syntax', body, { TARGET_SHA: 'x' }, false],
    ['moved head', { ...body, head: { ...body.head, sha: '0'.repeat(40) } }, {}, false],
    ['foreign head', { ...body, head: { ...body.head, repo: { full_name: 'other/repo' } } }, {}, false],
    ['closed PR', { ...body, state: 'closed' }, {}, false],
    ['wrong base branch', { ...body, base: { ...body.base, ref: 'dev' } }, {}, false],
    ['moved base SHA', { ...body, base: { ...body.base, sha: '0'.repeat(40) } }, {}, false],
    ['foreign base', { ...body, base: { ...body.base, repo: { full_name: 'other/repo' } } }, {}, false],
  ];
  for (const [name, response, changes, success] of cases) {
    writeFileSync(env.FIXTURE_JSON, JSON.stringify(response));
    const result = spawnSync('bash', ['-c', preflight], { env: { ...env, ...changes }, encoding: 'utf8' });
    assert.equal(result.status === 0, success, `${name}: ${result.stderr}`);
  }
});

test('checkout gate verifies real Git ancestry, not only API branch labels', t => {
  const directory = fixture(t);
  const env = { ...process.env, GIT_AUTHOR_NAME: 'mochi-the-kitty', GIT_COMMITTER_NAME: 'mochi-the-kitty',
    GIT_AUTHOR_EMAIL: '306965878+mochi-the-kitty@users.noreply.github.com',
    GIT_COMMITTER_EMAIL: '306965878+mochi-the-kitty@users.noreply.github.com' };
  const git = (...args) => execFileSync('git', args, { cwd: directory, env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  git('init'); git('checkout', '-b', 'source');
  git('commit', '--allow-empty', '-m', 'base'); const ancestor = git('rev-parse', 'HEAD');
  git('commit', '--allow-empty', '-m', 'head'); const head = git('rev-parse', 'HEAD');
  const script = source.split("node <<'NODE'")[0];
  const check = (target, base) => spawnSync('bash', ['-c', script], { cwd: directory, env: { ...env, TARGET_SHA: target, REVIEWED_BASE: base }, encoding: 'utf8' }).status;
  assert.equal(check(head, ancestor), 0);
  assert.notEqual(check('0'.repeat(40), ancestor), 0);
  assert.notEqual(check(head, '0'.repeat(40)), 0);
  git('checkout', '--orphan', 'unrelated'); git('commit', '--allow-empty', '-m', 'unrelated');
  assert.notEqual(check(git('rev-parse', 'HEAD'), ancestor), 0);
});
