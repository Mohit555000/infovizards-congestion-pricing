import { dateOnly, addDays, dates, isoWeekday, monday, monthEnd, dayHours, hourOccurrences, tollWindow, knownIssues } from './calendar.mjs';

export function integer(value, name = 'count') {
  if ((typeof value !== 'string' && typeof value !== 'number') || !/^\d+$/.test(String(value))) throw new Error(`Invalid ${name}: ${value}`);
  const n = Number(value);
  if (!Number.isSafeInteger(n)) throw new Error(`Unsafe ${name}`);
  return n;
}
export function unique(rows, fields) {
  const keys = new Set();
  for (const row of rows) {
    const key = JSON.stringify(fields.map((f) => row[f]));
    if (keys.has(key)) throw new Error(`Duplicate output key ${fields}: ${key}`);
    keys.add(key);
  }
}
export function parseSample(row) {
  const date = dateOnly(row.toll_date);
  const block = row.toll_10_minute_block;
  if (typeof block !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00\.000$/.test(block) || block.slice(0, 10) !== date) throw new Error('Invalid floating block timestamp');
  const hour = integer(row.hour_of_day); const minute = integer(row.minute_of_hour);
  if (hour > 23 || ![0, 10, 20, 30, 40, 50].includes(minute) || Number(block.slice(11, 13)) !== hour || Number(block.slice(14, 16)) !== minute) throw new Error('Block/hour/minute mismatch');
  if (row.toll_hour !== `${date}T${String(hour).padStart(2, '0')}:00:00.000`) throw new Error('Toll hour mismatch');
  if (integer(row.day_of_week_int) !== isoWeekday(date) % 7 + 1 || row.time_period !== tollWindow(date, hour)) throw new Error('Weekday/toll category mismatch');
  const sourceSunday = addDays(date, -(isoWeekday(date) % 7));
  if (dateOnly(row.toll_week) !== sourceSunday) throw new Error('Source week mismatch');
  for (const field of ['vehicle_class', 'detection_group', 'detection_region']) if (!row[field]) throw new Error(`Missing ${field}`);
  return { date, hour, minute, crz: integer(row.crz_entries), excluded: integer(row.excluded_roadway_entries) };
}
export function parseAggregate(row, hourly = false) {
  const date = dateOnly(row.toll_date);
  const r = { date, crz: integer(row.crz), excluded: integer(row.excluded), n_rows: integer(row.n_rows), n_blocks: integer(row.n_blocks), n_crz: integer(row.n_crz), n_excluded: integer(row.n_excluded) };
  if (hourly) {
    r.hour = integer(row.hour_of_day);
    if (r.hour > 23 || row.time_period !== tollWindow(date, r.hour)) throw new Error('Invalid hourly period');
    r.toll_window = row.time_period;
  } else {
    Object.assign(r, { crossing: row.detection_group, detector_region: row.detection_region, vehicle_class: row.vehicle_class });
    if (!r.crossing || !r.detector_region || !r.vehicle_class) throw new Error('Missing aggregate category');
  }
  return r;
}
export function validateDuplicateBlocks(rows, start, end, crossings, classes) {
  // Only the two occurrences in the repeated NYC fall-back hour may share a wall key.
  const actual = new Set();
  for (const row of rows) {
    const stamp = row.toll_10_minute_block;
    const date = stamp.slice(0, 10);
    if (dayHours(date) !== 25 || stamp.slice(11, 13) !== '01' || integer(row.n) !== 2) throw new Error(`Unexpected repeated block: ${JSON.stringify(row)}`);
    actual.add(JSON.stringify([stamp, row.detection_group, row.vehicle_class]));
  }
  for (const date of dates(start, end).filter((d) => dayHours(d) === 25)) {
    for (const crossing of crossings) for (const cls of classes) for (let m = 0; m < 60; m += 10) {
      const key = JSON.stringify([`${date}T01:${String(m).padStart(2, '0')}:00.000`, crossing.detection_group, cls]);
      if (!actual.delete(key)) throw new Error(`Missing repeated-hour multiplicity: ${key}`);
    }
  }
  if (actual.size) throw new Error('Unexpected duplicate categories');
}
const pct = (value, base) => base > 0 ? 100 * (value - base) / base : null;
const sum = (rows, field) => rows.reduce((s, r) => s + r[field], 0);
const meta = (flags = [], missing = null) => ({ quality_status: missing ? 'partial' : 'observed', missing_reason: missing, quality_flags: flags });
function grouped(rows, key) {
  const map = new Map();
  for (const r of rows) { const k = key(r); if (!map.has(k)) map.set(k, []); map.get(k).push(r); }
  return map;
}
export function trailing7(rows) {
  const byDate = new Map(rows.map((r) => [r.date, r]));
  return rows.map((r) => {
    const window = Array.from({ length: 7 }, (_, i) => byDate.get(addDays(r.date, -i)));
    const complete = window.filter((x) => x?.complete).length;
    return { ...r, ma7_complete_days: complete, crz_ma7: complete === 7 ? sum(window, 'crz_entries') / 7 : null };
  });
}
export function buildProducts(dailyInput, hourlyInput, { start, end, crossings, classes }) {
  unique(dailyInput, ['date', 'crossing', 'vehicle_class']);
  unique(hourlyInput, ['date', 'hour']);
  const nCategories = crossings.length * classes.length;
  const crossingMap = new Map(crossings.map((r) => [r.detection_group, r.detection_region]));
  const dailyMap = new Map(dailyInput.map((r) => [JSON.stringify([r.date, r.crossing, r.vehicle_class]), r]));
  const hourMap = new Map(hourlyInput.map((r) => [`${r.date}/${r.hour}`, r]));
  const cells = [], hourCells = [], daily = [];
  for (const date of dates(start, end)) {
    const nHours = dayHours(date); const expected = nHours * 6; const distinct = Math.min(nHours, 24) * 6;
    const dayCells = [];
    for (const crossing of crossings) for (const cls of classes) {
      const r = dailyMap.get(JSON.stringify([date, crossing.detection_group, cls]));
      if (r && (r.detector_region !== crossing.detection_region || !classes.includes(r.vehicle_class))) throw new Error('Category mapping drift');
      const complete = Boolean(r && r.n_rows === expected && r.n_blocks === distinct && r.n_crz === expected && r.n_excluded === expected);
      const cell = { date, crossing: crossing.detection_group, detector_region: crossing.detection_region, vehicle_class: cls, crz: r?.crz ?? null, excluded: r?.excluded ?? null, complete, observed_rows: r?.n_rows ?? 0, expected_rows: expected };
      cells.push(cell); dayCells.push(cell);
    }
    const dayHour = [];
    for (let hour = 0; hour < 24; hour++) {
      const occurrences = hourOccurrences(date, hour); const r = hourMap.get(`${date}/${hour}`);
      const expectedRows = occurrences * 6 * nCategories;
      const complete = occurrences > 0 && Boolean(r && r.n_rows === expectedRows && r.n_blocks === 6 && r.n_crz === expectedRows && r.n_excluded === expectedRows);
      const cell = { date, hour, toll_window: tollWindow(date, hour), crz: r?.crz ?? null, excluded: r?.excluded ?? null, complete, hour_occurrences: occurrences,
        observed_rows: r?.n_rows ?? 0, expected_rows: expectedRows, missing_reason: occurrences === 0 ? 'dst_nonexistent_wall_hour' : complete ? null : 'incomplete_hour' };
      if (occurrences === 0 && r) throw new Error(`Source hour exists in nonexistent NYC hour: ${date}`);
      hourCells.push(cell); dayHour.push(cell);
    }
    const complete = dayCells.every((r) => r.complete) && dayHour.filter((r) => r.hour_occurrences > 0).every((r) => r.complete);
    const crz = dayCells.some((r) => r.crz !== null) ? sum(dayCells, 'crz') : null;
    const excluded = dayCells.some((r) => r.excluded !== null) ? sum(dayCells, 'excluded') : null;
    if (crz !== null && (sum(dayHour, 'crz') !== crz || sum(dayHour, 'excluded') !== excluded)) throw new Error(`Daily/hourly reconciliation failed: ${date}`);
    daily.push({ date, day_of_week: isoWeekday(date), crz_entries: crz, excluded_roadway_entries: excluded, complete,
      observed_blocks: sum(dayCells, 'observed_rows'), expected_blocks: expected * nCategories,
      ...meta(knownIssues(date), complete ? null : 'incomplete_or_missing_day') });
  }
  if (dailyInput.length !== dailyMap.size || dailyInput.some((r) => !crossingMap.has(r.crossing) || !classes.includes(r.vehicle_class) || r.date < start || r.date > end)) throw new Error('Unexpected input categories/range');
  let q1 = trailing7(daily);
  const dateMap = new Map(q1.map((r) => [r.date, r]));
  q1 = q1.map((r) => {
    const candidate = addDays(r.date, -364); const referenceDate = candidate.startsWith('2025-') ? candidate : null;
    const ref = referenceDate ? dateMap.get(referenceDate) : null;
    const valid = r.complete && ref?.complete;
    return { ...r, reference_date: referenceDate, reference_year: referenceDate ? 2025 : null, comparison_rule: 'same_weekday_364_days_reference_2025',
      reference_entries: ref?.complete ? ref.crz_entries : null, yoy_pct: valid ? pct(r.crz_entries, ref.crz_entries) : null,
      comparison_status: !referenceDate || !ref?.complete ? 'unavailable_reference' : !r.complete ? 'incomplete_post' : ref.crz_entries === 0 ? 'zero_baseline' : 'comparable' };
  });
  const cellMap = new Map(cells.map((r) => [JSON.stringify([r.date, r.crossing, r.vehicle_class]), r]));
  const regionCells = grouped(cells, (r) => `${r.date}/${r.detector_region}`);
  const weekStarts = [...new Set(dates(start, end).map(monday))];
  const weekly = [];
  for (const week of weekStarts) for (const crossing of crossings) for (const cls of classes) for (const kind of ['crz', 'excluded']) {
    const weekEnd = addDays(week, 6);
    const items = dates(week, weekEnd).map((date) => dailyMap.get(JSON.stringify([date, crossing.detection_group, cls])));
    const checked = dates(week, weekEnd).map((date) => cellMap.get(JSON.stringify([date, crossing.detection_group, cls])));
    const completeDays = checked.filter((x) => x?.complete).length;
    weekly.push({ week_start: week, week_end: weekEnd, crossing: crossing.detection_group, detector_region: crossing.detection_region, vehicle_class: cls, count_kind: kind,
      entries: items.some(Boolean) ? sum(items.filter(Boolean), kind) : null, n_complete_days: completeDays,
      ...meta(knownIssues(week, weekEnd, crossing.detection_group, cls), completeDays === 7 ? null : 'partial_week') });
  }
  const baselineStarts = ['2025-01-06', '2025-01-13', '2025-01-20', '2025-01-27'];
  const baseGroups = grouped(weekly.filter((r) => baselineStarts.includes(r.week_start)), (r) => JSON.stringify([r.crossing, r.vehicle_class, r.count_kind]));
  const classTotals = grouped(weekly, (r) => JSON.stringify([r.week_start, r.vehicle_class, r.count_kind]));
  const q3 = weekly.map((r) => {
    const b = baseGroups.get(JSON.stringify([r.crossing, r.vehicle_class, r.count_kind])) ?? [];
    const count = b.filter((x) => x.n_complete_days === 7).length;
    const avg = count === 4 ? sum(b, 'entries') / 4 : null;
    const all = classTotals.get(JSON.stringify([r.week_start, r.vehicle_class, r.count_kind]));
    const total = all.length === crossings.length && all.every((x) => x.n_complete_days === 7) ? sum(all, 'entries') : null;
    return { ...r, baseline_week_count: count, baseline_avg_weekly_entries: avg, index_vs_launch: r.n_complete_days === 7 && avg > 0 ? 100 * r.entries / avg : null,
      class_total_entries: total, share_of_class_pct: total > 0 && r.n_complete_days === 7 ? 100 * r.entries / total : null,
      baseline_status: count !== 4 ? 'incomplete_reference' : avg === 0 ? 'zero_baseline' : 'complete_reference' };
  });
  const monthlyHours = grouped(hourCells, (r) => r.date.slice(0, 7));
  const q2 = [];
  for (const [month, rows] of monthlyHours) for (let dow = 1; dow <= 7; dow++) for (let hour = 0; hour < 24; hour++) {
    const selected = rows.filter((r) => isoWeekday(r.date) === dow && r.hour === hour);
    const observed = selected.filter((r) => r.complete);
    const expectedDates = dates(`${month}-01`, monthEnd(month)).filter((date) => isoWeekday(date) === dow && hourOccurrences(date, hour) > 0);
    const monthComplete = `${month}-01` >= start && monthEnd(month) <= end && dates(`${month}-01`, monthEnd(month)).every((d) => dateMap.get(d)?.complete);
    const complete = monthComplete && observed.length === expectedDates.length && selected.every((r) => r.complete || r.hour_occurrences === 0);
    const n = observed.length;
    q2.push({ window_id: month.slice(5), window_start: `${month}-01`, window_end: monthEnd(month), post_year: Number(month.slice(0, 4)), iso_weekday: dow, hour, view_mode: 'observed',
      entries: n ? sum(observed, 'crz') : null, n_days: n, expected_days: expectedDates.length, observed_hour_occurrences: sum(observed, 'hour_occurrences'),
      avg_entries_per_day: n ? sum(observed, 'crz') / n : null, toll_window: tollWindow(selected[0]?.date ?? expectedDates[0] ?? `${month}-01`, hour),
      ...meta(knownIssues(`${month}-01`, monthEnd(month)), complete ? null : 'partial_or_missing_exposure') });
  }
  const hourProfiles = new Map(q2.map((r) => [JSON.stringify([r.window_id, r.post_year, r.iso_weekday, r.hour]), r]));
  for (const r of q2) {
    const ref = hourProfiles.get(JSON.stringify([r.window_id, r.post_year - 1, r.iso_weekday, r.hour]));
    r.reference_year = r.post_year === 2026 ? 2025 : null;
    r.comparison_rule = 'same_calendar_month_weekday_means_2026_vs_2025';
    r.reference_avg = r.post_year === 2026 && !ref?.missing_reason ? ref?.avg_entries_per_day ?? null : null;
    r.difference_entries_per_day = !r.missing_reason && r.reference_avg !== null ? r.avg_entries_per_day - r.reference_avg : null;
  }
  const regions = [...new Set(crossings.map((r) => r.detection_region))].sort();
  const months = [...monthlyHours.keys()];
  const q7 = [];
  for (const month of months) for (const region of regions) for (const kind of ['crz', 'excluded']) {
    const monthDates = dates(`${month}-01`, monthEnd(month)); const wanted = crossings.filter((r) => r.detection_region === region).length * classes.length;
    let completeDays = 0, total = 0, observedDays = 0;
    for (const date of monthDates) {
      const selected = regionCells.get(`${date}/${region}`) ?? [];
      if (selected.some((r) => r[kind] !== null)) { observedDays++; total += sum(selected, kind); }
      if (selected.length === wanted && selected.every((r) => r.complete)) completeDays++;
    }
    q7.push({ month, detector_region: region, count_kind: kind, entries: observedDays ? total : null, n_complete_days: completeDays, observed_days: observedDays,
      expected_calendar_days: monthDates.length, avg_daily_entries: completeDays === observedDays && completeDays > 0 ? total / completeDays : null,
      ...meta(knownIssues(`${month}-01`, monthEnd(month)), completeDays === monthDates.length ? null : 'partial_or_missing_month') });
  }
  const monthMap = new Map(q7.map((r) => [JSON.stringify([r.month, r.detector_region, r.count_kind]), r]));
  for (const r of q7) {
    const refMonth = r.month.startsWith('2026-') ? `2025-${r.month.slice(5)}` : null;
    const ref = monthMap.get(JSON.stringify([refMonth, r.detector_region, r.count_kind]));
    r.reference_month = refMonth; r.reference_year = refMonth ? 2025 : null;
    r.comparison_rule = 'same_calendar_month_2026_vs_2025_daily_mean';
    r.reference_avg_daily_entries = ref && !ref.missing_reason ? ref.avg_daily_entries : null;
    r.yoy_pct = !r.missing_reason && r.reference_avg_daily_entries !== null ? pct(r.avg_daily_entries, r.reference_avg_daily_entries) : null;
    r.comparison_status = !ref || ref.missing_reason ? 'unavailable_reference' : r.missing_reason ? 'partial_post_month' : ref.avg_daily_entries === 0 ? 'zero_baseline' : 'comparable';
  }
  const outputs = { q1_mta_daily: q1, q2_mta_hour_weekday: q2, q3_mta_weekly_crossing_class: q3, q7_mta_region_month: q7 };
  unique(q1, ['date']); unique(q2, ['window_id', 'post_year', 'iso_weekday', 'hour', 'view_mode']); unique(q3, ['week_start', 'crossing', 'vehicle_class', 'count_kind']); unique(q7, ['month', 'detector_region', 'count_kind']);
  return { outputs, cells, hourCells };
}
