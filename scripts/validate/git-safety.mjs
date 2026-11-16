import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../../', import.meta.url));
const git = (args, input) => execFileSync('git', args, {
  cwd: root, encoding: 'utf8', input, stdio: ['pipe', 'pipe', 'pipe']
});
const splitNull = (value) => value.split('\0').filter(Boolean);

// Nonexistent probe paths check whole ignore classes without creating secrets or data.
const privatePaths = [
  'AGENTS.md', 'PLAN.md', 'STATUS.md', 'DECISIONS.md', 'PROMPTS.md', 'METHODOLOGY_AUDIT.md',
  'notebooks/nyc-congestion-pricing.html', 'notebooks/files/q1.mjs',
  'src/viz/q1.mjs', 'scripts/notebooks/package-q1.mjs', 'data/processed/tlc/sources/probe.parquet',
  'reference-docs/probe.pdf', 'milestones/probe.pdf', 'data/raw/probe.csv',
  'node_modules/probe/index.js', '.cache/npm/probe', '.npm/probe',
  '.observable/cache/probe', 'notebooks/.observable/cache/probe',
  '.tools/probe', '.local/probe', 'tmp/probe', 'coverage/probe', 'dist/probe',
  '.env', '.env.local', '.npmrc', '.netrc', '.aws/credentials',
  '.ssh/id_ed25519', 'credentials/probe.json', 'secrets/probe.json',
  'probe.pem', 'probe.key', 'probe.p12', 'probe.pfx',
  'analysis.duckdb', 'analysis.duckdb.wal', 'analysis.wal',
  '.agents/probe', '.codex/probe', '.DS_Store'
];
const publicPaths = [
  'README.md', '.gitignore', 'package.json', 'package-lock.json',
  '.nvmrc', '.node-version', 'src/tlc/processing.mjs',
  'scripts/validate/environment.mjs', 'scripts/validate/git-safety.mjs',
  'tests/dependencies.test.mjs', 'notebooks/files/q1_mta_daily.json',
  'data/processed/.gitkeep', 'data/geo/.gitkeep'
];
const failures = [];

function ignored(paths) {
  if (!paths.length) return new Set();
  try {
    return new Set(splitNull(git(['check-ignore', '--no-index', '-z', '--stdin'], paths.join('\0') + '\0')));
  } catch (error) {
    if (error.status === 1) return new Set();
    throw error;
  }
}

try {
  const top = git(['rev-parse', '--show-toplevel']).trim();
  if (path.resolve(top) !== path.resolve(root)) failures.push('Project is inside a different Git repository.');
  const ignoredPrivate = ignored(privatePaths);
  for (const file of privatePaths) {
    if (!ignoredPrivate.has(file)) failures.push(`Private path is not ignored: ${file}`);
  }
  for (const file of ignored(publicPaths)) failures.push(`Public path is ignored: ${file}`);

  // Ignore rules alone do not protect files already staged or tracked.
  const tracked = splitNull(git(['ls-files', '--cached', '-z']));
  for (const file of ignored(tracked)) failures.push(`Ignored private path is staged/tracked: ${file}`);
  console.log(`Repository: ${top}`);
  console.log(`Checked ${privatePaths.length} private-path rules and ${publicPaths.length} public paths.`);
  console.log(`Staged/tracked paths inspected: ${tracked.length}`);
} catch (error) {
  failures.push(`Git safety inspection failed: ${error.message}`);
}

if (failures.length) {
  for (const failure of failures) console.error(`FAIL: ${failure}`);
  process.exitCode = 1;
} else {
  console.log('PASS: private paths ignored, public README trackable, no private paths staged/tracked.');
}
