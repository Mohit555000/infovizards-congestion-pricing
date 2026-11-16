import {mkdir} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {DuckDBInstance} from '@duckdb/node-api';
import {acquire, importLocal} from '../../src/tlc/source.mjs';

const args = process.argv.slice(2);
if (args.length && (args.length !== 2 || args[0] !== '--local-march-2026' || !args[1])) {
  throw new Error('Usage: node scripts/tlc/prepare-sources.mjs [--local-march-2026 FILE]');
}
const cache = fileURLToPath(new URL('../../.cache/tlc/', import.meta.url));
const tempDirectory = path.join(cache, 'tmp');
await mkdir(tempDirectory, {recursive: true});
const db = await DuckDBInstance.create(':memory:', {
  extension_directory: path.join(cache, 'extensions'),
  temp_directory: tempDirectory,
  memory_limit: '1GB',
  threads: '2',
  autoinstall_known_extensions: 'false',
  autoload_known_extensions: 'false'
});
const connection = await db.connect();
try {
  if (args.length) {
    await importLocal(connection, cache, 'fhvhv', '2026-03', args[1]);
    console.log('Prepared local March 2026 HVFHV source.');
  } else {
    await connection.run('LOAD httpfs');
    await connection.run('SET http_timeout=60');
    await connection.run('SET http_retries=0');
    await acquire(connection, cache, 'yellow', '2024-03', false);
    console.log('Prepared March 2024 yellow taxi source for the cross-month regression.');
  }
} finally {
  connection.closeSync();
  db.closeSync();
}
