import { access, readFile, stat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../../', import.meta.url));
const requiredDirectories = [
  'src', 'scripts', 'scripts/validate', 'tests',
  'data/processed', 'data/geo', 'data/source/geography', 'notebooks/files'
];
const requiredFiles = [
  'README.md', '.gitignore', '.nvmrc', '.node-version',
  'package.json', 'package-lock.json', 'data/observable-attachments.json'
];

const failures = [];
if (Number(process.versions.node.split('.')[0]) !== 24 || !process.release.lts) {
  failures.push(`Use Node.js 24 LTS; current runtime is ${process.version}.`);
}
if (typeof globalThis.fetch !== 'function') {
  failures.push('Native Node.js fetch is unavailable.');
}
for (const directory of requiredDirectories) {
  try {
    if (!(await stat(path.join(root, directory))).isDirectory()) {
      failures.push(`Not a directory: ${directory}`);
    }
  } catch {
    failures.push(`Missing directory: ${directory}`);
  }
}
for (const file of requiredFiles) {
  try {
    await access(path.join(root, file), constants.R_OK);
  } catch {
    failures.push(`Missing or unreadable file: ${file}`);
  }
}

try {
  const packageJson = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
  const lock = JSON.parse(await readFile(path.join(root, 'package-lock.json'), 'utf8'));
  for (const [name, version] of Object.entries(packageJson.dependencies)) {
    if (lock.packages?.[`node_modules/${name}`]?.version !== version) {
      failures.push(`Lockfile version mismatch: ${name}`);
    }
    const installed = JSON.parse(await readFile(path.join(root, 'node_modules', name, 'package.json'), 'utf8'));
    if (installed.version !== version) failures.push(`Installed version mismatch: ${name}`);
  }
} catch (error) {
  failures.push(`Dependency inspection failed: ${error.message}`);
}

try {
  const git = execFileSync('git', ['--version'], { encoding: 'utf8' }).trim();
  console.log(git);
} catch {
  failures.push('Git is unavailable on PATH.');
}

console.log(`Project: ${root}`);
console.log(`Runtime: ${process.version} (${process.release.lts ?? 'not LTS'})`);
console.log(`Node executable: ${process.execPath}`);
const npmAgent = process.env.npm_config_user_agent;
if (npmAgent) {
  console.log(`Package runner: ${npmAgent}`);
  const major = Number(npmAgent.match(/\bnpm\/(\d+)/)?.[1]);
  if (!Number.isInteger(major) || major < 11) failures.push('Run checks with npm 11 or newer.');
}
if (failures.length) {
  for (const failure of failures) console.error(`FAIL: ${failure}`);
  process.exitCode = 1;
} else {
  console.log('PASS: setup files, Node LTS, native fetch and dependency versions. No data requested.');
}
