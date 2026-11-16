import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { csvParse } from 'd3';
import { DuckDBInstance } from '@duckdb/node-api';
import { digest } from '../../src/mta/api.mjs';
import { unique } from '../../src/mta/transform.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const range = process.argv[2] ?? '2025-01-05_2026-11-07';
if (!/^\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}$/.test(range)) throw new Error('Expected START_END directory name');
const base = path.join(root, 'data/processed/mta', range);
const manifest = JSON.parse(await readFile(path.join(base, 'manifest.json')));
const tables = {};
for (const file of manifest.files) {
  const body = await readFile(path.join(base, file.filename));
  assert.equal(digest(body), file.sha256); assert.equal(body.length, file.bytes);
  if (file.filename.endsWith('.json')) {
    const rows = JSON.parse(body); assert.equal(rows.length, file.rows); tables[file.filename.slice(0, -5)] = rows;
  }
}
for (const [name, rows] of Object.entries(tables)) {
  const csv = csvParse(await readFile(path.join(base, name + '.csv'), 'utf8'));
  assert.equal(csv.length, rows.length);
  for (let i = 0; i < rows.length; i++) for (const [key, value] of Object.entries(rows[i])) {
    assert.equal(csv[i][key], value === null ? '' : Array.isArray(value) ? JSON.stringify(value) : String(value));
  }
}
const daily = tables.q1_mta_daily; const byDate = new Map(daily.map((r) => [r.date, r]));
unique(daily, ['date']); unique(tables.q2_mta_hour_weekday, ['window_id', 'post_year', 'iso_weekday', 'hour', 'view_mode']);
unique(tables.q3_mta_weekly_crossing_class, ['week_start', 'crossing', 'vehicle_class', 'count_kind']); unique(tables.q7_mta_region_month, ['month', 'detector_region', 'count_kind']);
const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-8, `${a} != ${b}`);
for (let i = 0; i < daily.length; i++) {
  const row = daily[i];
  // Independent index-based arithmetic checks also explicitly verify consecutive dates.
  const window = daily.slice(Math.max(0, i - 6), i + 1);
  const continuous = window.length === 7 && Date.parse(window[6].date) - Date.parse(window[0].date) === 6 * 86400000 && window.every((r) => r.complete);
  if (continuous) near(row.crz_ma7, window.reduce((s, r) => s + r.crz_entries, 0) / 7);
  else assert.equal(row.crz_ma7, null);
  if (row.yoy_pct !== null) {
    const ref = byDate.get(row.reference_date);
    assert.equal(row.reference_year, 2025); assert.ok(ref.complete && row.complete);
    assert.equal(Date.parse(row.date) - Date.parse(ref.date), 364 * 86400000);
    near(row.yoy_pct, 100 * (row.crz_entries / ref.crz_entries - 1));
  }
}
// Independently reaggregate daily/hour detail for every observed weekday/hour cell.
for (const row of tables.q2_mta_hour_weekday) {
  const eligible = tables.mta_daily_hour.filter((h) => h.complete && h.hour === row.hour && h.date.slice(0, 7) === `${row.post_year}-${row.window_id}` && (new Date(`${h.date}T00:00:00Z`).getUTCDay() || 7) === row.iso_weekday);
  assert.equal(row.n_days, eligible.length);
  if (eligible.length) {
    const total = eligible.reduce((n, h) => n + h.crz, 0);
    assert.equal(row.entries, total); near(row.avg_entries_per_day, total / eligible.length);
    assert.equal(row.observed_hour_occurrences, eligible.reduce((n, h) => n + h.hour_occurrences, 0));
  } else assert.equal(row.avg_entries_per_day, null);
}
const database = await DuckDBInstance.create(':memory:', { autoinstall_known_extensions: 'false', autoload_known_extensions: 'false' });
const connection = await database.connect();
let sqlWeeks;
try {
  const filename = path.join(base, 'mta_daily_crossing_class.json').replaceAll("'", "''");
  const result = await connection.runAndReadAll(`SELECT strftime(date_trunc('week', date::DATE), '%Y-%m-%d') AS week_start,crossing,vehicle_class,sum(crz) AS crz,sum(excluded) AS excluded,count(*) FILTER(WHERE complete) AS days FROM read_json_auto('${filename}') GROUP BY 1,2,3 ORDER BY 1,2,3`);
  sqlWeeks = result.getRowObjectsJson();
} finally { connection.closeSync(); database.closeSync(); }
const weekMap = new Map(sqlWeeks.map((r) => [JSON.stringify([r.week_start, r.crossing, r.vehicle_class]), r]));
const shares = new Map(); const launchIndices = new Map();
for (const row of tables.q3_mta_weekly_crossing_class) {
  const sql = weekMap.get(JSON.stringify([row.week_start, row.crossing, row.vehicle_class]));
  assert.ok(sql); assert.equal(row.entries, Number(sql[row.count_kind])); assert.equal(row.n_complete_days, Number(sql.days));
  const baseline = ['2025-01-06', '2025-01-13', '2025-01-20', '2025-01-27'].map((week) => weekMap.get(JSON.stringify([week, row.crossing, row.vehicle_class])));
  const valid = baseline.length === 4 && baseline.every((r) => r && Number(r.days) === 7);
  if (valid) near(row.baseline_avg_weekly_entries, baseline.reduce((s, r) => s + Number(r[row.count_kind]), 0) / 4);
  else assert.equal(row.index_vs_launch, null);
  if (row.index_vs_launch !== null) near(row.index_vs_launch, row.entries / row.baseline_avg_weekly_entries * 100);
  if (row.share_of_class_pct !== null) {
    const key = JSON.stringify([row.week_start, row.vehicle_class, row.count_kind]);
    shares.set(key, (shares.get(key) ?? 0) + row.share_of_class_pct);
  }
  if (row.index_vs_launch !== null && ['2025-01-06', '2025-01-13', '2025-01-20', '2025-01-27'].includes(row.week_start)) {
    const key = JSON.stringify([row.crossing, row.vehicle_class, row.count_kind]);
    launchIndices.set(key, (launchIndices.get(key) ?? 0) + row.index_vs_launch / 4);
  }
}
for (const value of shares.values()) near(value, 100);
for (const value of launchIndices.values()) near(value, 100);
for (const row of tables.q7_mta_region_month) {
  const expectedDays = new Date(Date.UTC(+row.month.slice(0, 4), +row.month.slice(5), 0)).getUTCDate();
  assert.equal(row.expected_calendar_days, expectedDays);
  if (row.yoy_pct !== null) {
    assert.equal(row.n_complete_days, expectedDays); assert.equal(row.reference_year, 2025);
    const ref = tables.q7_mta_region_month.find((r) => r.month === row.reference_month && r.detector_region === row.detector_region && r.count_kind === row.count_kind);
    assert.equal(ref.n_complete_days, ref.expected_calendar_days);
    near(row.yoy_pct, 100 * (row.avg_daily_entries / ref.avg_daily_entries - 1));
  }
}
const report = { validated_at: new Date().toISOString(), range, checks: ['all output SHA256 and byte lengths', 'all CSV/JSON cell equivalence', 'all four product primary keys', 'seven consecutive-day arithmetic', 'independent hour-weekday means and exposure', 'daily reference labels and ratio arithmetic', 'independent DuckDB Monday-week counts', 'all launch-reference means and indices', 'class shares sum to100', 'four-launch-index mean100', 'complete-month reference ratios'],
  status: 'passed', files: manifest.files.length, official_daily_controls: manifest.validation.official_daily_controls,
  complete_dates: manifest.coverage.complete_dates, baseline_complete_series: manifest.coverage.baseline_complete_series };
const reportDirectory = path.join(root, '.cache/mta/runs', range);
await mkdir(reportDirectory, {recursive: true});
await writeFile(path.join(reportDirectory, 'output-validation.json'), JSON.stringify(report, null, 2) + '\n');
console.log(JSON.stringify(report, null, 2));
