// SQL expression shared by the processor and controlled arithmetic fixtures.
// `checked` supplies parsed timestamps and the number of distinct versions per ID.
export function exclusionSql(year) {
  if (![2024, 2025, 2026].includes(year)) throw new Error('Unapproved Citi Bike year');
  return `CASE
    WHEN ride_id IS NULL OR trim(ride_id)='' THEN 'missing_ride_id'
    WHEN id_versions>1 THEN 'conflicting_ride_id'
    WHEN start_at IS NULL OR end_at IS NULL THEN 'invalid_timestamp'
    WHEN end_at<=start_at THEN 'nonpositive_duration'
    WHEN strftime(start_at,'%Y-%m')!='${year}-05' THEN 'outside_window'
    ELSE NULL END`;
}
