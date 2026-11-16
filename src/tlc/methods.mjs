export function adapter(service, columns) {
  const map = service === 'yellow' ? { pickup: 'tpep_pickup_datetime', dropoff: 'tpep_dropoff_datetime', miles: 'trip_distance', fare: 'fare_amount' }
    : service === 'fhvhv' ? { pickup: 'pickup_datetime', dropoff: 'dropoff_datetime', miles: 'trip_miles', fare: 'base_passenger_fare' } : null;
  if (!map) throw new Error('Unknown service');
  for (const name of [map.pickup, map.dropoff, 'PULocationID', 'DOLocationID']) if (!columns.includes(name)) throw new Error(`Missing required column ${name}`);
  return map;
}
export function zoneStatus(id) {
  return id === null ? 'missing' : !Number.isInteger(id) || id < 1 || id > 265 ? 'invalid' : id > 263 ? 'nonspatial' : 'spatial';
}
export function comparison(base, post, months, { basePresent = true, postPresent = true, complete = true } = {}) {
  if (!Number.isInteger(months) || months < 1) throw new Error('Invalid month denominator');
  const status = !basePresent || base === null ? 'missing_baseline' : !postPresent || post === null ? 'missing_post_period' : !complete ? 'partial_coverage' : base === 0 ? 'zero_baseline' : base / months < 1000 ? 'low_volume' : 'valid';
  return { comparison_status: status,
    baseline_status: !basePresent || base === null ? 'missing_baseline' : base === 0 ? 'zero_baseline' : 'observed_positive',
    post_status: !postPresent || post === null ? 'missing_post_period' : 'observed',
    period_coverage_status: complete ? 'complete' : 'partial_or_unverified',
    base_trips: base, post_trips: post, abs_change: base !== null && post !== null ? post - base : null,
    avg_base_trips_per_month: base === null ? null : base / months, low_volume: base === null ? null : base / months < 1000,
    pct_change: status === 'valid' ? 100 * (post - base) / base : null };
}
export function allowedMonth(month, { boundary = false } = {}) { if (!/^(2024|2025|2026)-(02|03|04|05)$/.test(month) || (month.endsWith('-02') && !boundary)) throw new Error('Outside approved March-May scope or February boundary source'); return month; }
export const literal = (v) => `'${String(v).replaceAll("'", "''")}'`;
export function dailyReference(date,rule='calendar_date') {
  if(!/^202[456]-\d\d-\d\d$/.test(date)||new Date(date+'T00:00:00Z').toISOString().slice(0,10)!==date)throw new Error('Invalid scoped date');
  if(rule==='calendar_date')return '2024'+date.slice(4);
  if(rule==='weekday_364'&&date.startsWith('2025-'))return new Date(Date.parse(date+'T00:00:00Z')-364*86400000).toISOString().slice(0,10);
  throw new Error('Unresolved daily mapping; no implicit 728-day fallback');
}

export const SERVICES = ['yellow', 'fhvhv'];
export const YEARS = [2024, 2025, 2026];
export const WINDOWS = [['03'], ['04'], ['05'], ['03','04'], ['04','05'], ['03','04','05']];
export function sourceTasks() {
  return SERVICES.flatMap(service => YEARS.flatMap(year => ['02','03','04','05'].map(m => ({service, month: `${year}-${m}`}))));
}
export function registerSource(registry, source) {
  const key = `${source.service}_${allowedMonth(source.month, {boundary: true})}`;
  if (registry.has(key)) throw new Error(`Duplicate ingestion: ${key}`);
  if ([...registry.values()].some(s => s.sha256 === source.sha256)) throw new Error(`Source content alias: ${key}`);
  registry.set(key, source);
}
// Source values stay naive TIMESTAMP. These flags interpret wall values in America/New_York;
// they do not establish an offset or replace/shift the stored timestamp.
export function dstSql(column) {
  return `CASE WHEN month(${column})=3 AND dayofweek(${column})=0 AND day(${column}) BETWEEN 8 AND 14 AND hour(${column})=2 THEN 'spring_gap' WHEN month(${column})=11 AND dayofweek(${column})=0 AND day(${column}) BETWEEN 1 AND 7 AND hour(${column})=1 THEN 'fall_fold' ELSE 'ordinary' END`;
}
export function eventSql(service, year) {
  return `SELECT ${literal(service)} AS service,metric,event_at::DATE::VARCHAR AS date,hour(event_at)::INTEGER AS hour,
    CASE WHEN location_id BETWEEN 1 AND 265 THEN location_id ELSE NULL END AS "LocationID",
    CASE WHEN location_id IS NULL THEN 'missing' WHEN location_id NOT BETWEEN 1 AND 265 THEN 'invalid' WHEN location_id>263 THEN 'nonspatial' ELSE 'spatial' END AS zone_status,
    ${dstSql('event_at')} AS temporal_quality,count(*)::BIGINT AS trips
    FROM trips, LATERAL (VALUES ('pickup',pickup_at,pickup_id),('dropoff',dropoff_at,dropoff_id)) e(metric,event_at,location_id)
    WHERE event_at>=TIMESTAMP '${year}-03-01' AND event_at<TIMESTAMP '${year}-06-01' GROUP BY ALL`;
}
export function groupFor(zone) {
  if (zone.LocationID > 263) return 'nonspatial';
  return zone.official_cbd_member ? 'inside' : 'outside_crz_adjacency_unresolved';
}
