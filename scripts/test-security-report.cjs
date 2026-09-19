const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { summarize } = require('./security-report.cjs');
const publish = require('./publish-security-report.cjs');

function fixture(t, overrides = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'security-report-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const files = {
    'audit.json': JSON.stringify({ vulnerabilities: { list: [], count: 0 }, warnings: {} }),
    'audit-exit.txt': '0', 'deny-exit.txt': '0',
    'deny.jsonl': JSON.stringify({ type: 'summary', fields: { advisories: { errors: 0 } } }),
    ...overrides,
  };
  for (const [name, value] of Object.entries(files)) if (value !== null) fs.writeFileSync(path.join(dir, name), value);
  return dir;
}

test('clean means both scanners completed without findings', t => {
  assert.match(summarize(fixture(t)), /## Security scan clean/);
});

test('findings include vulnerabilities and maintenance warnings with safe markdown', t => {
  const item = { package: { name: 'crate|@all', version: '1.0' },
    advisory: { id: 'RUSTSEC-2026-0285', title: '<script>[click](evil)' } };
  const dir = fixture(t, {
    'audit.json': JSON.stringify({ vulnerabilities: { list: [item], count: 1 }, warnings: { unmaintained: [item] } }),
    'audit-exit.txt': '1',
  });
  const report = summarize(dir);
  assert.match(report, /Security findings reported/);
  assert.match(report, /2 RustSec findings/);
  assert.match(report, /https:\/\/rustsec.org\/advisories\/RUSTSEC-2026-0285/);
  assert.ok(!report.includes('@all') && !report.includes('<script>') && !report.includes('[click](evil)'));
});

test('deny-only advisory errors are findings, not a scanner failure', t => {
  const dir = fixture(t, { 'deny-exit.txt': '1', 'deny.jsonl': [
    { type: 'diagnostic', fields: { severity: 'error', message: 'yanked dependency' } },
    { type: 'summary', fields: { advisories: { errors: 1 } } },
  ].map(JSON.stringify).join('\n') });
  assert.match(summarize(dir), /Security findings reported/);
});

test('missing, malformed and failed scans never report clean', t => {
  for (const overrides of [
    { 'audit.json': null }, { 'audit.json': '{' }, { 'audit.json': '{}' },
    { 'audit-exit.txt': '127' }, { 'audit-exit.txt': '1' },
    { 'deny-exit.txt': '1' }, { 'deny.jsonl': '' }, { 'deny.jsonl': 'not json' },
    { 'deny.jsonl': JSON.stringify({ type: 'log', fields: { level: 'ERROR', message: 'database unavailable' } }) },
    { 'audit.json': JSON.stringify({ vulnerabilities: { list: [null], count: 1 }, warnings: {} }) },
  ]) assert.match(summarize(fixture(t, overrides)), /## Security scan incomplete/);
});

function mock(t, { comments = [], head = 'abc123', runNumber = 2, attempt = 1, repo = 'org/repo' } = {}) {
  const writes = [];
  const listPRs = () => {};
  const listComments = () => {};
  const github = { rest: {
    pulls: { list: listPRs, get: async () => ({ data: { head: { sha: head, repo: { full_name: repo } } } }) },
    issues: { listComments, createComment: async args => writes.push(['create', args]), updateComment: async args => writes.push(['update', args]) },
  }, paginate: async method => method === listPRs ? [{ number: 12 }] : comments };
  const context = { repo: { owner: 'org', repo: 'repo' }, serverUrl: 'https://github.com', payload: { workflow_run: {
    event: 'pull_request', repository: { full_name: 'org/repo' },
    head_repository: { full_name: 'org/repo', owner: { login: 'org' } }, head_branch: 'feature',
    head_sha: 'abc123', run_number: runNumber, run_attempt: attempt, id: 123,
  } } };
  const oldDir = process.env.SECURITY_REPORT_DIR;
  process.env.SECURITY_REPORT_DIR = fixture(t);
  t.after(() => {
    if (oldDir === undefined) delete process.env.SECURITY_REPORT_DIR;
    else process.env.SECURITY_REPORT_DIR = oldDir;
  });
  return { args: { github, context, core: { info() {} } }, writes };
}
const comment = (body, login = 'github-actions[bot]') => ({ id: 42, user: { login }, body });
const marker = '<!-- rush-dependency-security-report -->';

test('first scan creates one comment; subsequent scan updates it to clean', async t => {
  const first = mock(t);
  await publish(first.args);
  assert.equal(first.writes[0][0], 'create');
  const next = mock(t, { comments: [comment(marker + '\nold findings')] });
  await publish(next.args);
  assert.equal(next.writes.length, 1);
  assert.equal(next.writes[0][0], 'update');
  assert.equal(next.writes[0][1].comment_id, 42);
  assert.match(next.writes[0][1].body, /Security scan clean/);
});

test('stale commits, foreign repos and older attempts never overwrite a report', async t => {
  for (const options of [
    { head: 'newer' }, { repo: 'other/repo' },
    { comments: [comment(marker + '\n<!-- scan-run:3 attempt:1 -->')] },
    { comments: [comment(marker + '\n<!-- scan-run:2 attempt:2 -->')] },
  ]) {
    const m = mock(t, options);
    await publish(m.args);
    assert.equal(m.writes.length, 0);
  }
});

test('a human cannot spoof the bot marker to have their comment overwritten', async t => {
  const m = mock(t, { comments: [comment(marker, 'contributor')] });
  await publish(m.args);
  assert.equal(m.writes[0][0], 'create');
});

test('fork PRs are resolved from GitHub metadata', async t => {
  const m = mock(t, { repo: 'contributor/repo' });
  m.args.context.payload.workflow_run.head_repository = {
    full_name: 'contributor/repo', owner: { login: 'contributor' },
  };
  await publish(m.args);
  assert.equal(m.writes[0][0], 'create');
});

test('missing scan artifacts replace an old result with incomplete', async t => {
  const m = mock(t, { comments: [comment(marker + '\n## Security scan clean')] });
  process.env.SECURITY_REPORT_DIR = path.join(process.env.SECURITY_REPORT_DIR, 'missing');
  await publish(m.args);
  assert.equal(m.writes[0][0], 'update');
  assert.match(m.writes[0][1].body, /## Security scan incomplete/);
});
