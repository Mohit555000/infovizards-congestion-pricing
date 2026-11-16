import { readFile, writeFile, mkdir, rename } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { csvFormat } from 'd3';
import { DuckDBInstance } from '@duckdb/node-api';
import { SocrataClient, METADATA, digest } from '../../src/mta/api.mjs';
import { scope, dateOnly, dates, monthEnd, addDays } from '../../src/mta/calendar.mjs';
import { integer, parseSample, parseAggregate, validateDuplicateBlocks, buildProducts, unique } from '../../src/mta/transform.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const args = process.argv.slice(2);
function arg(name, fallback) { const i = args.indexOf(`--${name}`); return i < 0 ? fallback : args[i + 1]; }
const allowed = new Set(['--start', '--end', '--page-size', '--offline']);
for (const [i, a] of args.entries()) if (a.startsWith('--') && !allowed.has(a)) throw new Error(`Unknown option ${a}`);
const { start, end } = scope(arg('start', '2025-01-05'), arg('end', '2026-11-07'));
const offline = args.includes('--offline');
const stamp = new Date().toISOString();
const tag = `${start}_${end}`;
const logDir = path.join(root, '.cache/mta/runs', tag);
await mkdir(logDir, { recursive: true });
let requestNo = 0;
const client = new SocrataClient({ cacheDir: path.join(root, '.cache/mta/metadata'), offline,
  pageSize: Number(arg('page-size', '5000')),
  onRequest: (url, attempt) => console.log(`MTA request ${++requestNo}${attempt ? ` retry ${attempt}` : ''}: ${new URL(url).pathname}`) });
const where = (a, b) => `toll_date >= '${a}T00:00:00' and toll_date < '${addDays(b, 1)}T00:00:00'`;
const sums = 'sum(crz_entries) as crz,sum(excluded_roadway_entries) as excluded,count(*) as n_rows,count(distinct toll_10_minute_block) as n_blocks,count(crz_entries) as n_crz,count(excluded_roadway_entries) as n_excluded';
const dc = 'toll_date,detection_group,detection_region,vehicle_class';
const hc = 'toll_date,hour_of_day,time_period';
let logFile;
try {
  if (offline) {
    const frozen = JSON.parse(await readFile(path.join(logDir, 'run.json'), 'utf8'));
    client.cacheDir = path.join(root, '.cache/mta/api', String(frozen.source_revision));
  }
  const metadata = await client.get(METADATA, { fresh: !offline });
  const fieldTypes = Object.fromEntries(metadata.columns.map((c) => [c.fieldName, c.dataTypeName]));
  const expected = { toll_date: 'calendar_date', toll_hour: 'calendar_date', toll_10_minute_block: 'calendar_date', minute_of_hour: 'number', hour_of_day: 'number', day_of_week_int: 'number', day_of_week: 'text', toll_week: 'calendar_date', time_period: 'text', vehicle_class: 'text', detection_group: 'text', detection_region: 'text', crz_entries: 'number', excluded_roadway_entries: 'number' };
  for (const [f, type] of Object.entries(expected)) if (fieldTypes[f] !== type) throw new Error(`Source schema drift at ${f}`);
  // Cache every research request by metadata revision: no cross-revision reuse.
  client.cacheDir = path.join(root, '.cache/mta/api', String(metadata.rowsUpdatedAt));
  const range = await client.query({ $select: 'min(toll_date) as first_date,max(toll_date) as last_date,count(*) as n_rows' });
  if (start < dateOnly(range[0].first_date) || end > dateOnly(range[0].last_date)) throw new Error('Requested range exceeds live source availability');
  const classes = (await client.query({ $select: 'vehicle_class', $group: 'vehicle_class', $order: 'vehicle_class', $where: where(start, end) })).map((r) => r.vehicle_class);
  const crossings = await client.query({ $select: 'detection_group,detection_region', $group: 'detection_group,detection_region', $order: 'detection_group,detection_region', $where: where(start, end) });
  unique(crossings, ['detection_group']);
  if (!classes.length || !crossings.length) throw new Error('No source categories available');
  const sample = await client.query({ $where: where(start, end), $order: 'toll_10_minute_block,detection_group,vehicle_class', $limit: '10' });
  sample.forEach(parseSample);
  const invalid = [
    ...Object.keys(expected).map((f) => `${f} is null`),
    'crz_entries < 0', 'excluded_roadway_entries < 0',
    'crz_entries != floor(crz_entries)', 'excluded_roadway_entries != floor(excluded_roadway_entries)',
    'hour_of_day < 0', 'hour_of_day > 23', 'minute_of_hour not in (0,10,20,30,40,50)',
    'date_trunc_ymd(toll_10_minute_block) != toll_date',
    'date_extract_hh(toll_10_minute_block) != hour_of_day',
    'date_extract_mm(toll_10_minute_block) != minute_of_hour',
    'date_extract_ss(toll_10_minute_block) != 0',
    'date_trunc_ymd(toll_hour) != toll_date', 'date_extract_hh(toll_hour) != hour_of_day',
    'date_extract_mm(toll_hour) != 0',
    'day_of_week_int != date_extract_dow(toll_date) + 1'
  ].join(' or ');
  const invalidRows = await client.query({ $select: 'count(*) as n', $where: `(${where(start, end)}) and (${invalid})` });
  if (integer(invalidRows[0].n)) throw new Error(`Invalid source records: ${invalidRows[0].n}; requires review, not silent exclusion`);
  const duplicateBlocks = await client.pages({ $select: 'toll_10_minute_block,detection_group,vehicle_class,count(*) as n', $where: where(start, end),
    $group: 'toll_10_minute_block,detection_group,vehicle_class', $having: 'count(*) > 1', $order: 'toll_10_minute_block,detection_group,vehicle_class' });
  validateDuplicateBlocks(duplicateBlocks, start, end, crossings, classes);
  const daily = [], hourly = [];
  let a = start;
  while (a <= end) {
    const b = monthEnd(a.slice(0, 7)) < end ? monthEnd(a.slice(0, 7)) : end;
    console.log(`Extracting ${a}..${b}`);
    daily.push(...(await client.pages({ $select: `${dc},${sums}`, $where: where(a, b), $group: dc, $order: dc })).map((r) => parseAggregate(r)));
    hourly.push(...(await client.pages({ $select: `${hc},${sums}`, $where: where(a, b), $group: hc, $order: hc })).map((r) => parseAggregate(r, true)));
    a = addDays(b, 1);
  }
  const { outputs, cells, hourCells } = buildProducts(daily, hourly, { start, end, crossings, classes });
  const controls = await client.pages({ $select: 'toll_date,sum(crz_entries) as crz,sum(excluded_roadway_entries) as excluded,count(*) as n_rows', $where: where(start, end), $group: 'toll_date', $order: 'toll_date' });
  const byDate = new Map(outputs.q1_mta_daily.map((r) => [r.date, r]));
  for (const control of controls) {
    const row = byDate.get(dateOnly(control.toll_date));
    if (row.crz_entries !== integer(control.crz) || row.excluded_roadway_entries !== integer(control.excluded) || row.observed_blocks !== integer(control.n_rows)) throw new Error(`Official direct daily control mismatch: ${control.toll_date}`);
  }
  if (controls.length !== byDate.size) console.log('WARNING: missing source dates explicitly represented');
  // Independent local SQL reconciliation against both API controls and JS outputs.
  const cubeFile = path.join(logDir, 'daily-cube.json');
  await writeFile(cubeFile, JSON.stringify(daily));
  const database = await DuckDBInstance.create(':memory:', { autoinstall_known_extensions: 'false', autoload_known_extensions: 'false' });
  const connection = await database.connect();
  try {
    const reader = await connection.runAndReadAll(`SELECT date,sum(crz) AS crz,sum(excluded) AS excluded FROM read_json_auto('${cubeFile.replaceAll("'", "''")}') GROUP BY date ORDER BY date`);
    for (const row of reader.getRowObjectsJson()) {
      const expectedRow = byDate.get(row.date);
      if (Number(row.crz) !== expectedRow.crz_entries || Number(row.excluded) !== expectedRow.excluded_roadway_entries) throw new Error('Independent DuckDB reconciliation failed');
    }
  } finally { connection.closeSync(); database.closeSync(); }
  const rawHour = await client.pages({ $where: `${where(start, start)} and hour_of_day=5`, $order: 'toll_10_minute_block,detection_group,vehicle_class' });
  const parsedHour = rawHour.map(parseSample);
  const localHour = hourCells.find((r) => r.date === start && r.hour === 5);
  if (!localHour || parsedHour.length !== localHour.observed_rows || parsedHour.reduce((n, r) => n + r.crz, 0) !== localHour.crz || parsedHour.reduce((n, r) => n + r.excluded, 0) !== localHour.excluded) throw new Error('Raw ten-minute sample vs hourly aggregate mismatch');
  const after = await client.get(METADATA, { fresh: !offline });
  if (after.rowsUpdatedAt !== metadata.rowsUpdatedAt || after.tableId !== metadata.tableId) throw new Error('Source changed during extraction; outputs not promoted. Re-run under new revision.');
  // Also export the daily/hour detail needed to reaggregate arbitrary supported selections.
  outputs.mta_daily_crossing_class = cells;
  outputs.mta_daily_hour = hourCells;
  const runId = `mta-${start}-${end}-r${metadata.rowsUpdatedAt}-v1.1.0`;
  const target = path.join(root, 'data/processed/mta', tag);
  const stage = path.join(root, '.cache/mta/staging', tag);
  await mkdir(stage, { recursive: true });
  const files = [];
  for (const [name, rawRows] of Object.entries(outputs)) {
    const rows = rawRows.map((r) => ({ schema_version: '1.1.0', run_id: runId, ...r }));
    // Field order follows the explicit product schema; never inferred from arbitrary map order.
    for (const extension of ['json', 'csv']) {
      const body = extension === 'json' ? `${JSON.stringify(rows, null, 2)}\n` : `${csvFormat(rows.map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, Array.isArray(v) ? JSON.stringify(v) : v]))))}\n`;
      const filename = `${name}.${extension}`;
      await writeFile(path.join(stage, filename), body);
      files.push({ filename, rows: rows.length, bytes: Buffer.byteLength(body), sha256: digest(body) });
    }
  }
  const coverage = { requested_dates: dates(start, end).length, source_dates: controls.length,
    complete_dates: outputs.q1_mta_daily.filter((r) => r.complete).length,
    missing_or_partial_dates: outputs.q1_mta_daily.filter((r) => !r.complete).map((r) => r.date),
    partial_weeks: [...new Set(outputs.q3_mta_weekly_crossing_class.filter((r) => r.n_complete_days !== 7).map((r) => r.week_start))],
    partial_months: [...new Set(outputs.q7_mta_region_month.filter((r) => r.missing_reason).map((r) => r.month))],
    baseline_complete_series: outputs.q3_mta_weekly_crossing_class.filter((r) => r.week_start === '2025-01-06' && r.baseline_week_count === 4).length };
  const manifest = { run_id: runId, execution_mode: offline ? 'offline_replay' : 'network_allowed', schema_version: '1.1.0', extraction_started_at: stamp, extraction_finished_at: new Date().toISOString(),
    source: METADATA, source_revision: metadata.rowsUpdatedAt, source_table_id: metadata.tableId, source_live_range: range[0],
    requested_start: start, fixed_cutoff: end, approved_maximum: '2026-11-07',
    timezone: 'America/New_York', timezone_semantics: 'publisher floating wall dates retained; NYC DST coverage checked; offsets/fold are not encoded by source',
    classes, crossings, coverage, validation: { invalid_source_rows: 0, repeated_wall_keys: duplicateBlocks.length, official_daily_controls: controls.length, js_hourly_daily_reconciliation: 'passed', independent_duckdb_daily_reconciliation: 'passed', raw_hour_control_date: start, raw_hour_control_hour: 5, raw_hour_control_rows: rawHour.length, raw_hour_reconciliation: 'passed', source_revision_stable: true },
    files, requests: client.log };
  await writeFile(path.join(stage, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
  await mkdir(target, { recursive: true });
  for (const f of [...files.map((f) => f.filename), 'manifest.json']) await rename(path.join(stage, f), path.join(target, f));
  await writeFile(path.join(logDir, 'raw-hour-sample.json'), JSON.stringify(rawHour, null, 2));
  await writeFile(path.join(logDir, 'source-sample.json'), JSON.stringify(sample, null, 2));
  await writeFile(path.join(logDir, 'duplicate-wall-keys.json'), JSON.stringify(duplicateBlocks, null, 2));
  await writeFile(path.join(logDir, 'official-daily-controls.json'), JSON.stringify(controls, null, 2));
  await writeFile(path.join(logDir, 'metadata.json'), JSON.stringify(metadata, null, 2));
  logFile = path.join(logDir, 'run.json');
  await writeFile(logFile, JSON.stringify(manifest, null, 2));
  console.log(JSON.stringify({ target, files, coverage, validation: manifest.validation }, null, 2));
} catch (error) {
  await writeFile(path.join(logDir, 'failure.json'), JSON.stringify({ start, end, failed_at: new Date().toISOString(), error: error.message, requests: client.log }, null, 2));
  console.error(error); process.exitCode = 1;
}
