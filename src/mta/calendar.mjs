// Calendar arithmetic uses UTC as a host-independent calendar representation only.
// Source floating wall timestamps are never converted into UTC instants.
export function dateOnly(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}(?:T00:00:00(?:\.000)?)?$/.test(value)) throw new Error(`Invalid date: ${value}`);
  const date = value.slice(0, 10);
  const parsed = new Date(`${date}T00:00:00Z`);
  if (!Number.isFinite(+parsed) || parsed.toISOString().slice(0, 10) !== date) throw new Error(`Invalid calendar date: ${value}`);
  return date;
}
export function addDays(date, days) {
  return new Date(+new Date(`${dateOnly(date)}T00:00:00Z`) + days * 86400000).toISOString().slice(0, 10);
}
export function dates(start, end) {
  const rows = [];
  for (let d = dateOnly(start); d <= dateOnly(end); d = addDays(d, 1)) rows.push(d);
  return rows;
}
export const isoWeekday = (date) => new Date(`${dateOnly(date)}T00:00:00Z`).getUTCDay() || 7;
export const monday = (date) => addDays(date, 1 - isoWeekday(date));
export const monthEnd = (month) => new Date(Date.UTC(Number(month.slice(0, 4)), Number(month.slice(5, 7)), 0)).toISOString().slice(0, 10);
const local = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23' });
function midnight(date) {
  const base = +new Date(`${date}T00:00:00Z`);
  for (let h = 0; h <= 8; h++) {
    const time = base + h * 3600000;
    const p = Object.fromEntries(local.formatToParts(time).map((x) => [x.type, x.value]));
    if (`${p.year}-${p.month}-${p.day}` === date && p.hour === '00') return time;
  }
  throw new Error(`Cannot determine NYC midnight: ${date}`);
}
export function dayHours(date) { return (midnight(addDays(date, 1)) - midnight(date)) / 3600000; }
export function hourOccurrences(date, hour) {
  const n = dayHours(date);
  return hour === 2 && n === 23 ? 0 : hour === 1 && n === 25 ? 2 : 1;
}
export function tollWindow(date, hour) {
  return hour >= (isoWeekday(date) <= 5 ? 5 : 9) && hour < 21 ? 'Peak' : 'Overnight';
}
export function knownIssues(start, end = start, crossing = null, vehicleClass = null) {
  const flags = [];
  if (start <= '2025-05-08' && end >= '2025-04-07' && (!crossing || crossing === 'Queens Midtown Tunnel')) flags.push('midtown_equipment_issue_2025_04_07_to_05_08');
  if (start <= '2025-03-31' && end >= '2025-01-05' && (!vehicleClass || /^[23] -/.test(vehicleClass))) flags.push('class_2_3_historical_revision');
  return flags;
}
export function scope(start, end) {
  dateOnly(start); dateOnly(end);
  if (start < '2025-01-05' || end > '2026-11-07' || start > end) throw new Error('Range must be inside approved 2025-01-05..2026-11-07');
  return { start, end };
}
