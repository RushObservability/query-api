const fs = require('node:fs');
const path = require('node:path');

// Scan artifacts are untrusted data, including when read by workflow_run.
function read(dir, name) {
  const file = path.join(dir, name);
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.size > 5_000_000) throw new Error('Invalid report file');
  return fs.readFileSync(file, 'utf8');
}

function escape(value) {
  return String(value ?? '').slice(0, 400)
    .replace(/[\r\n\t]/g, ' ')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/@/g, '&#64;').replace(/[\\`*_{}\[\]()#+.!|~-]/g, '\\$&');
}

function summarize(dir) {
  const rows = [];
  const unknown = [];
  let audit;
  try {
    audit = JSON.parse(read(dir, 'audit.json'));
    const exit = read(dir, 'audit-exit.txt').trim();
    if (!Array.isArray(audit.vulnerabilities?.list) ||
        !audit.warnings || typeof audit.warnings !== 'object' ||
        Array.isArray(audit.warnings) || !['0', '1'].includes(exit)) throw new Error();
    if (exit === '1' && audit.vulnerabilities.list.length === 0) throw new Error();
    if (audit.vulnerabilities.count !== audit.vulnerabilities.list.length) throw new Error();
    const findings = audit.vulnerabilities.list.map(item => ['Vulnerability', item]);
    for (const [kind, items] of Object.entries(audit.warnings)) {
      if (!Array.isArray(items)) throw new Error();
      for (const item of items) findings.push([kind, item]);
    }
    if (findings.some(([, item]) => !item || typeof item !== 'object')) throw new Error();
    rows.push(...findings);
  } catch {
    unknown.push('cargo-audit');
  }
  const diagnostics = [];
  try {
    const exit = read(dir, 'deny-exit.txt').trim();
    const lines = read(dir, 'deny.jsonl').split('\n').filter(line => line.trim());
    let completed = false;
    for (const line of lines) {
      const entry = JSON.parse(line);
      const fields = entry.fields;
      if (!fields || typeof fields !== 'object') throw new Error();
      if (entry.type === 'log' && fields.level?.toLowerCase() === 'error') throw new Error();
      if (entry.type === 'summary' && Number.isInteger(fields.advisories?.errors)) completed = true;
      if (entry.type === 'diagnostic' && ['error', 'warning'].includes(fields.severity)) {
        diagnostics.push(fields);
      }
    }
    if (!completed || !['0', '1'].includes(exit) ||
        (exit === '1' && !diagnostics.some(d => d.severity === 'error'))) throw new Error();
  } catch {
    unknown.push('cargo-deny advisories');
  }
  const status = unknown.length ? 'Security scan incomplete' :
    rows.length || diagnostics.length ? 'Security findings reported' : 'Security scan clean';
  const lines = [
    `## ${status}`, '',
    'Dependency advisories are informational and do not block this PR.', '',
    `${rows.length} RustSec findings; ${diagnostics.length} cargo-deny advisory diagnostics.`,
  ];
  if (unknown.length) lines.push('', `Could not complete: ${unknown.join(', ')}. This is not a clean scan.`);
  if (rows.length) {
    lines.push('', '| Kind | Package | Advisory | Details |', '|---|---|---|---|');
    for (const [kind, item] of rows.slice(0, 30)) {
      const id = item.advisory?.id;
      const advisory = /^RUSTSEC-\d{4}-\d{4}$/.test(id) ? `[${id}](https://rustsec.org/advisories/${id})` : escape(id);
      lines.push(`| ${escape(kind)} | ${escape(item.package?.name)} ${escape(item.package?.version)} | ${advisory} | ${escape(item.advisory?.title || item.kind)} |`);
    }
  }
  if (diagnostics.length) {
    lines.push('', '<details><summary>cargo-deny advisories</summary>', '');
    for (const d of diagnostics.slice(0, 20)) lines.push(`- ${escape(d.severity)}: ${escape(d.message)}`);
    lines.push('', '</details>');
  }
  lines.push('', 'Full results, including findings omitted from this summary, are in the `dependency-security-report` artifact.',
    'Reviewed exception: RUSTSEC-2023-0071. Release security gates are unchanged.');
  return lines.join('\n') + '\n';
}

module.exports = { summarize };
if (require.main === module) {
  const report = summarize(process.argv[2]);
  if (!report.startsWith('## Security scan clean\n')) {
    console.log('::warning::Dependency security report needs review. See the job summary and scan artifacts.');
  }
  process.stdout.write(report);
  if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, report);
}
