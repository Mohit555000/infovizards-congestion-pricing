import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';

const root = new URL('../../', import.meta.url);
const bytes = path => readFile(new URL(path, root));
const json = async path => JSON.parse(await bytes(path));
const sha256 = value => createHash('sha256').update(value).digest('hex');

const notebook = await json('data/observable-attachments.json');
assert.equal(notebook.files.length, 16);
assert.equal(new Set(notebook.files.map(file => file.path)).size, 16);
for (const file of notebook.files) {
  const [attachment, source] = await Promise.all([bytes(file.path), bytes(file.source)]);
  assert.equal(attachment.length, file.bytes, file.path);
  assert.equal(sha256(attachment), file.sha256, file.path);
  assert.ok(attachment.equals(source), file.source);
}

const geography = await json('data/geo/v1/source-manifest.json');
const snapshots = geography.snapshot_files.filter(file => !file.path.endsWith('.pdf'));
for (const file of snapshots) {
  const snapshot = await bytes('data/source/geography/' + file.path.split('/').at(-1));
  assert.equal(snapshot.length, file.bytes, file.path);
  assert.equal(sha256(snapshot), file.sha256, file.path);
}

const tlc = await json('data/processed/tlc/release-manifest.json');
assert.equal(tlc.sources.length, 24);
assert.equal(new Set(tlc.sources.map(file => file.url)).size, 24);
for (const file of tlc.files) {
  assert.equal(sha256(await bytes('data/processed/tlc/' + file.path)), file.sha256, file.path);
}
console.log(`Verified ${notebook.files.length} Observable attachments, ${snapshots.length} original geography files and ${tlc.files.length} TLC release tables.`);
