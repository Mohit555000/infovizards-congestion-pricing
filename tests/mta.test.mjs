import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { SocrataClient } from '../src/mta/api.mjs';
import { addDays, dates, isoWeekday, monday, dayHours, hourOccurrences, tollWindow, scope } from '../src/mta/calendar.mjs';
import { integer, parseSample, parseAggregate, trailing7, unique, buildProducts, validateDuplicateBlocks } from '../src/mta/transform.mjs';

const testCache = new URL('../.cache/mta-tests/', import.meta.url);
await mkdir(testCache, { recursive: true });
const fixtureDir = () => mkdtemp(path.join(testCache.pathname, 'fixture-'));

// SYNTHETIC SOFTWARE FIXTURES ONLY. No values below are NYC observations.
const crossings = [{ detection_group: 'Synthetic A', detection_region: 'Synthetic Region' }, { detection_group: 'Synthetic B', detection_region: 'Synthetic Region' }];
const classes = ['Synthetic Class'];
function fixture(start, end, mutate = () => {}) {
  const daily = [], hourly = [];
  dates(start, end).forEach((date, i) => {
    const value = i + 1, hours = dayHours(date), blocks = hours * 6;
    crossings.forEach((crossing, c) => daily.push({ date, crossing: crossing.detection_group, detector_region: crossing.detection_region, vehicle_class: classes[0], crz: value * hours * (c + 1), excluded: 0,
      n_rows: blocks, n_blocks: Math.min(hours, 24) * 6, n_crz: blocks, n_excluded: blocks }));
    for (let hour = 0; hour < 24; hour++) {
      const n = hourOccurrences(date, hour);
      if (n) hourly.push({ date, hour, toll_window: tollWindow(date, hour), crz: value * n * 3, excluded: 0, n_rows: n * 12, n_blocks: 6, n_crz: n * 12, n_excluded: n * 12 });
    }
  });
  mutate(daily, hourly);
  return buildProducts(daily, hourly, { start, end, crossings, classes });
}
test('strict numeric parsing rejects null, fractions, negatives and unsafe values', () => {
  assert.equal(integer('0'), 0);
  for (const value of [null, undefined, '', '-1', '1.5', NaN, 9007199254740992]) assert.throws(() => integer(value));
});
test('floating sample parsing checks block, weekday and toll category without timezone conversion', () => {
  const row = { toll_date: '2025-01-06T00:00:00.000', toll_10_minute_block: '2025-01-06T05:10:00.000', toll_hour: '2025-01-06T05:00:00.000', hour_of_day: '5', minute_of_hour: '10', day_of_week_int: '2', toll_week: '2025-01-05T00:00:00.000', time_period: 'Peak', detection_group: 'Test', detection_region: 'Test', vehicle_class: 'Test', crz_entries: '4', excluded_roadway_entries: '2' };
  assert.deepEqual(parseSample(row), { date: '2025-01-06', hour: 5, minute: 10, crz: 4, excluded: 2 });
  assert.throws(() => parseSample({ ...row, toll_10_minute_block: row.toll_10_minute_block + 'Z' }));
  assert.throws(() => parseSample({ ...row, time_period: 'Overnight' }));
  assert.throws(() => parseSample({ ...row, minute_of_hour: '9' }));
});
test('aggregate parsing rejects unsafe counts and missing categories', () => {
  const r = { toll_date: '2025-01-06T00:00:00.000', crz: '1', excluded: '0', n_rows: '144', n_blocks: '144', n_crz: '144', n_excluded: '144', detection_group: 'A', detection_region: 'R', vehicle_class: 'C' };
  assert.equal(parseAggregate(r).crz, 1);
  assert.throws(() => parseAggregate({ ...r, detection_group: null }));
});
test('daily and hourly totals reconcile to independent arithmetic; excluded zeros stay observed', () => {
  const { outputs, hourCells } = fixture('2025-01-06', '2025-01-07');
  assert.equal(outputs.q1_mta_daily[0].crz_entries, 24 * (1 + 2));
  assert.equal(outputs.q1_mta_daily[1].crz_entries, 24 * (2 + 4));
  assert.equal(outputs.q1_mta_daily[0].excluded_roadway_entries, 0);
  assert.equal(hourCells[0].crz, 3);
  assert.ok(outputs.q1_mta_daily.every((r) => r.complete));
});
test('trailing seven-calendar-day mean has six null initial windows and exact arithmetic', () => {
  const rows = dates('2025-01-01', '2025-01-08').map((date, i) => ({ date, complete: true, crz_entries: i + 1 }));
  const out = trailing7(rows);
  assert.ok(out.slice(0, 6).every((r) => r.crz_ma7 === null));
  assert.equal(out[6].crz_ma7, 4); assert.equal(out[7].crz_ma7, 5);
  assert.equal(trailing7(rows.filter((r) => r.date !== '2025-01-04')) .at(-1).crz_ma7, null);
  rows[3].complete = false;
  assert.equal(trailing7(rows).at(-1).crz_ma7, null);
});
test('Monday boundaries and four complete launch weeks gate baseline, index and crossing shares', () => {
  assert.equal(monday('2025-01-05'), '2024-12-30');
  assert.equal(monday('2025-01-12'), '2025-01-06');
  const { outputs } = fixture('2025-01-06', '2025-02-02');
  const row = outputs.q3_mta_weekly_crossing_class.find((r) => r.week_start === '2025-01-06' && r.crossing === 'Synthetic A' && r.count_kind === 'crz');
  // Weeks have fixture day indices 1..7,8..14,15..21,22..28.
  const baseline = (28 + 77 + 126 + 175) * 24 / 4;
  assert.equal(row.baseline_week_count, 4);
  assert.equal(row.baseline_avg_weekly_entries, baseline);
  assert.equal(row.index_vs_launch, 100 * 28 * 24 / baseline);
  assert.ok(Math.abs(row.share_of_class_pct - 100 / 3) < 1e-10);
  const partial = fixture('2025-01-06', '2025-02-01').outputs.q3_mta_weekly_crossing_class;
  assert.ok(partial.every((r) => r.index_vs_launch === null && r.baseline_week_count === 3));
});
test('missing dates remain unknown and invalidate references and seven-day windows', () => {
  const { outputs } = fixture('2025-01-06', '2025-01-13', (d, h) => {
    for (const a of [d, h]) for (let i = a.length - 1; i >= 0; i--) if (a[i].date === '2025-01-09') a.splice(i, 1);
  });
  const missing = outputs.q1_mta_daily.find((r) => r.date === '2025-01-09');
  assert.equal(missing.crz_entries, null); assert.equal(missing.complete, false);
  assert.equal(outputs.q1_mta_daily.at(-1).crz_ma7, null);
});
test('duplicate output keys and mismatched aggregate totals fail', () => {
  assert.throws(() => unique([{ date: 'a' }, { date: 'a' }], ['date']));
  assert.throws(() => fixture('2025-01-06', '2025-01-06', (d) => d.push(d[0])));
  assert.throws(() => fixture('2025-01-06', '2025-01-06', (d, h) => h[0].crz++));
});
test('partial monthly periods do not receive valid monthly comparisons', () => {
  const { outputs } = fixture('2025-01-06', '2025-01-12');
  assert.ok(outputs.q7_mta_region_month.every((r) => r.missing_reason && r.yoy_pct === null));
  assert.ok(outputs.q2_mta_hour_weekday.every((r) => r.missing_reason && r.difference_entries_per_day === null));
});
test('DST exposure preserves repeated hour counts and represents nonexistent hour as unavailable', () => {
  assert.equal(dayHours('2025-03-09'), 23); assert.equal(dayHours('2025-11-02'), 25);
  assert.equal(hourOccurrences('2025-03-09', 2), 0); assert.equal(hourOccurrences('2025-11-02', 1), 2);
  assert.equal(fixture('2025-03-09', '2025-03-09').outputs.q1_mta_daily[0].crz_entries, 23 * 3);
  const fall = fixture('2025-11-02', '2025-11-02');
  assert.equal(fall.outputs.q1_mta_daily[0].crz_entries, 25 * 3);
  assert.equal(fall.hourCells[1].crz, 6);
  assert.throws(() => validateDuplicateBlocks([{ toll_10_minute_block: '2025-01-06T01:00:00.000', detection_group: 'Synthetic A', vehicle_class: classes[0], n: '2' }], '2025-01-06', '2025-01-06', crossings, classes));
});
test('weekday/weekend toll boundaries are precise', () => {
  assert.equal(tollWindow('2025-01-06', 4), 'Overnight'); assert.equal(tollWindow('2025-01-06', 5), 'Peak');
  assert.equal(tollWindow('2025-01-05', 8), 'Overnight'); assert.equal(tollWindow('2025-01-05', 9), 'Peak');
  assert.equal(tollWindow('2025-01-06', 21), 'Overnight');
});
test('364-day reference is labeled; missing references and zero baselines suppress ratios', () => {
  const out = fixture('2025-01-05', '2026-01-04').outputs;
  const r = out.q1_mta_daily.at(-1);
  assert.equal(r.reference_date, '2025-01-05'); assert.equal(r.reference_year, 2025);
  assert.equal(r.yoy_pct, 36400);
  const missing = fixture('2026-01-04', '2026-01-04').outputs.q1_mta_daily[0];
  assert.equal(missing.yoy_pct, null); assert.equal(missing.comparison_status, 'unavailable_reference');
});
test('host timezone cannot shift service dates or week boundaries', () => {
  const code = `import {monday,dayHours} from './src/mta/calendar.mjs'; console.log(JSON.stringify([monday('2025-01-05'),dayHours('2025-03-09')]))`;
  const run = (TZ) => execFileSync(process.execPath, ['--input-type=module', '-e', code], { cwd: new URL('../', import.meta.url), env: { ...process.env, TZ }, encoding: 'utf8' });
  assert.equal(run('UTC'), run('Pacific/Honolulu')); assert.equal(run('Asia/Tokyo'), run('America/New_York'));
});
test('scope cannot expand beyond approved dates', () => {
  assert.throws(() => scope('2024-01-01', '2025-01-05')); assert.throws(() => scope('2025-01-05', '2026-11-08'));
  assert.throws(() => scope('2025-02-30', '2025-03-01'));
});
test('HTTP client retries transient errors, paginates, verifies cached payload and supports offline reuse', async () => {
  const cacheDir = await fixtureDir();
  let calls = 0;
  const fetchFn = async (url) => {
    calls++;
    if (calls === 1) return new Response('busy', { status: 429 });
    const offset = Number(new URL(url).searchParams.get('$offset'));
    return new Response(JSON.stringify(offset === 0 ? [{ id: 1 }, { id: 2 }] : [{ id: 3 }]));
  };
  const client = new SocrataClient({ cacheDir, fetchFn, pageSize: 2, sleep: async () => {} });
  assert.equal((await client.pages({ $order: 'id' })).length, 3); assert.equal(calls, 3);
  const offline = new SocrataClient({ cacheDir, pageSize: 2, offline: true });
  assert.equal((await offline.pages({ $order: 'id' })).length, 3);
  await assert.rejects(() => offline.query({ $limit: '1' }), /Offline cache miss/);
  await assert.rejects(() => client.pages({}), /ordering/);
});
test('HTTP client fails permanent errors and oversized responses without endless retries', async () => {
  const cacheDir = await fixtureDir();
  let calls = 0;
  const c = new SocrataClient({ cacheDir, fetchFn: async () => { calls++; return new Response('{}', { status: 400 }); }, sleep: async () => {} });
  await assert.rejects(() => c.query({}), /HTTP 400/); assert.equal(calls, 1);
  const capped = new SocrataClient({ cacheDir, fetchFn: async () => new Response('1234567890'), maxBytes: 4 });
  await assert.rejects(() => capped.query({}), /byte cap/);
});

test('hour-weekday means use actual exposure dates and monthly totals use calendar days', () => {
  const { outputs } = fixture('2025-02-01', '2025-02-28');
  const mondayHour = outputs.q2_mta_hour_weekday.find((r) => r.iso_weekday === 1 && r.hour === 9);
  assert.equal(mondayHour.n_days, 4);
  assert.equal(mondayHour.entries, (3 + 10 + 17 + 24) * 3);
  assert.equal(mondayHour.avg_entries_per_day, (3 + 10 + 17 + 24) * 3 / 4);
  assert.equal(mondayHour.missing_reason, null);
  const month = outputs.q7_mta_region_month.find((r) => r.count_kind === 'crz');
  assert.equal(month.entries, (28 * 29 / 2) * 24 * 3);
  assert.equal(month.avg_daily_entries, (28 * 29 / 2) * 24 * 3 / 28);
  assert.equal(month.n_complete_days, 28);
});
test('zero-valued complete reference has null percentage/index, not missing-source status', () => {
  const { outputs } = fixture('2025-01-05', '2026-01-04', (d, h) => {
    for (const row of [...d, ...h]) row.crz = 0;
  });
  assert.equal(outputs.q1_mta_daily.at(-1).reference_entries, 0);
  assert.equal(outputs.q1_mta_daily.at(-1).comparison_status, 'zero_baseline');
  assert.equal(outputs.q1_mta_daily.at(-1).yoy_pct, null);
  const week = outputs.q3_mta_weekly_crossing_class.find((r) => r.week_start === '2025-01-06' && r.count_kind === 'crz');
  assert.equal(week.baseline_status, 'zero_baseline');
  assert.equal(week.index_vs_launch, null); assert.equal(week.share_of_class_pct, null);
});
test('non-DST duplicate profiles and cached-payload tampering fail safely', async () => {
  const { readdir } = await import('node:fs/promises');
  const cacheDir = await fixtureDir();
  const c = new SocrataClient({ cacheDir, fetchFn: async () => new Response('[{"id":1}]') });
  await c.query({ $limit: '1' });
  const file = path.join(cacheDir, (await readdir(cacheDir))[0]);
  const record = JSON.parse(await readFile(file, 'utf8')); record.body = '[{"id":2}]';
  await writeFile(file, JSON.stringify(record));
  await assert.rejects(() => c.query({ $limit: '1' }), /integrity/);
});
test('valid repeated-hour multiplicities are retained and missing repeated blocks are rejected', () => {
  const rows = [];
  for (const cross of crossings) for (const cls of classes) for (let min = 0; min < 60; min += 10) rows.push({ toll_10_minute_block: `2025-11-02T01:${String(min).padStart(2, '0')}:00.000`, detection_group: cross.detection_group, vehicle_class: cls, n: '2' });
  assert.doesNotThrow(() => validateDuplicateBlocks(rows, '2025-11-02', '2025-11-02', crossings, classes));
  assert.throws(() => validateDuplicateBlocks(rows.slice(1), '2025-11-02', '2025-11-02', crossings, classes));
});
