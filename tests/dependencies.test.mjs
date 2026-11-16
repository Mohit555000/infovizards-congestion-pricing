import assert from 'node:assert/strict';
import test from 'node:test';
import { DuckDBInstance } from '@duckdb/node-api';
import { scaleLinear } from 'd3';

test('D3 loads as an ES module and evaluates a scale', () => {
  const scale = scaleLinear().domain([0, 1]).range([0, 100]);
  assert.equal(scale(0.5), 50);
});

test('native DuckDB executes an in-memory constant query', async () => {
  const database = await DuckDBInstance.create(':memory:', {
    autoinstall_known_extensions: 'false',
    autoload_known_extensions: 'false'
  });
  let connection;
  try {
    connection = await database.connect();
    const reader = await connection.runAndReadAll('SELECT version() AS engine_version, 6 * 7 AS answer');
    const rows = reader.getRowObjects();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].answer, 42);
    assert.match(rows[0].engine_version, /^v1\.5\.6/);
  } finally {
    connection?.closeSync();
    database.closeSync();
  }
});
