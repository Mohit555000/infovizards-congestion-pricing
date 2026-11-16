// Output-only refinement of the frozen mapping. It does not change official CBD
// membership, event aggregation, CitiBike assignments or candidate adjacency rules.
export function groupFor(zone) {
  if (zone.LocationID > 263) return 'nonspatial';
  if (zone.official_cbd_member === true) return 'inside';
  if (zone.official_cbd_member !== false) throw new Error(`Unclassified spatial CBD membership: ${zone.LocationID}`);
  if (zone.zone_group === 'outside' && zone.classification_status === 'outside_operational_groups' && zone.polygon_relation === 'outside_geofence') return 'outside';
  if (['proposed_operational_adjacency', 'ambiguous'].includes(zone.classification_status)) return 'unresolved';
  throw new Error(`Unsupported geographic classification: ${zone.LocationID}`);
}
export function geographyStatus(zones) {
  return {
    version: 'official-cbd-v1-outside-review-2026-11-15',
    verified_inside_ids: zones.filter(z => groupFor(z) === 'inside').map(z => z.LocationID),
    ordinary_outside_ids: zones.filter(z => groupFor(z) === 'outside').map(z => z.LocationID),
    adjacency_unresolved_ids: zones.filter(z => groupFor(z) === 'unresolved').map(z => z.LocationID),
    candidate_labels_not_adopted: zones.filter(z => groupFor(z) === 'unresolved').map(z => ({LocationID: z.LocationID, zone: z.zone, candidate: z.zone_group, status: z.classification_status})),
    interpretation: '215ordinary outside under the existing frozen mapping; ten unresolved boundary/adjacency subgroup cases. Official nonmember union remains225. No proposed north/river membership adopted.',
    warning: 'Official taxi-zone endpoint approximation, not road crossings or toll liability'
  };
}
