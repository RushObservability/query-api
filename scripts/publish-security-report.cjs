const { summarize } = require('./security-report.cjs');
const marker = '<!-- rush-dependency-security-report -->';

module.exports = async function publish({ github, context, core }) {
  const run = context.payload.workflow_run;
  const { owner, repo } = context.repo;
  if (run.event !== 'pull_request' || run.repository.full_name !== `${owner}/${repo}`) return;
  // Resolve the PR from GitHub metadata, never from attacker-controlled artifacts.
  const candidates = await github.paginate(github.rest.pulls.list, {
    owner, repo, state: 'open', head: `${run.head_repository.owner.login}:${run.head_branch}`, per_page: 100,
  });
  for (const pr of candidates) {
    const current = (await github.rest.pulls.get({ owner, repo, pull_number: pr.number })).data;
    if (current.head.repo?.full_name !== run.head_repository.full_name || current.head.sha !== run.head_sha) {
      core.info('Skipping stale scan for an older PR commit.');
      continue;
    }
    const comments = await github.paginate(github.rest.issues.listComments, {
      owner, repo, issue_number: pr.number, per_page: 100,
    });
    const existing = comments.find(c => c.user?.login === 'github-actions[bot]' && c.body?.startsWith(marker));
    const previous = existing?.body.match(/<!-- scan-run:(\d+) attempt:(\d+) -->/);
    if (previous && (+previous[1] > run.run_number ||
        (+previous[1] === run.run_number && +previous[2] > run.run_attempt))) continue;
    const body = `${marker}\n<!-- scan-run:${run.run_number} attempt:${run.run_attempt} -->\n` +
      summarize(process.env.SECURITY_REPORT_DIR) +
      `\nCommit: \`${run.head_sha.slice(0, 12)}\` · [Scan details and artifacts](${context.serverUrl}/${owner}/${repo}/actions/runs/${run.id})\n` +
      '\nThis comment is updated in place after each scan.\n';
    if (existing) {
      if (existing.body !== body) await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id, body });
    } else {
      await github.rest.issues.createComment({ owner, repo, issue_number: pr.number, body });
    }
  }
};
