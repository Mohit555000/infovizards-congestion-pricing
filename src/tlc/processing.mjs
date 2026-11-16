import assert from 'node:assert/strict';
import path from 'node:path';
import {mkdir} from 'node:fs/promises';
import {literal as q,eventSql,dstSql} from './methods.mjs';
import {save} from './source.mjs';
export const query = async(c,sql)=>(await c.runAndReadAll(sql)).getRowObjectsJson();
export async function regression(c, cache) {
  // Real cached March2024 data; same production SQL, independently specified boundary count.
  const file=path.join(cache,'yellow_2024-03/normalized.parquet');
  await c.run(`CREATE OR REPLACE TEMP TABLE trips AS SELECT * FROM read_parquet(${q(file)}) WHERE dropoff_at>=TIMESTAMP '2024-04-01' AND dropoff_at<TIMESTAMP '2024-05-01'`);
  const expected=Number((await query(c,'SELECT count(*) n FROM trips'))[0].n);
  assert.equal(expected,631,'Frozen March2024 yellow April dropoff control changed');
  await c.run(`CREATE OR REPLACE TEMP TABLE regression_events AS ${eventSql('yellow',2024)}`);
  const actual=Number((await query(c,"SELECT coalesce(sum(trips),0) n FROM regression_events WHERE metric='dropoff' AND date LIKE '2024-04-%'"))[0].n);
  assert.equal(actual,expected);await c.run('DROP TABLE regression_events');await c.run('DROP TABLE trips');
  return {status:'passed',source:file,expected_april_dropoffs:expected,actual_april_dropoffs:actual,production_event_sql:true};
}
export async function processSource(c, source, staging) {
  const {service,month,file}=source, year=Number(month.slice(0,4));
  // Exactly one read of each normalized source in the processing loop. All subsequent
  // metrics/profile scans use this temporary materialization, which can spill to disk.
  await c.run(`CREATE OR REPLACE TEMP TABLE trips AS SELECT *,file_row_number AS ingestion_row,epoch(dropoff_at-pickup_at) AS duration_seconds FROM read_parquet(${q(file)},file_row_number=true)`);
  const profile=(await query(c,`SELECT count(*) AS source_rows,count(DISTINCT ingestion_row) AS distinct_ingestion_rows,
    min(pickup_at)::VARCHAR first_pickup,max(pickup_at)::VARCHAR last_pickup,min(dropoff_at)::VARCHAR first_dropoff,max(dropoff_at)::VARCHAR last_dropoff,
    count(*) FILTER(WHERE pickup_at IS NULL) null_pickup_time,count(*) FILTER(WHERE dropoff_at IS NULL) null_dropoff_time,
    count(*) FILTER(WHERE pickup_id IS NULL) null_pickup_id,count(*) FILTER(WHERE dropoff_id IS NULL) null_dropoff_id,
    count(*) FILTER(WHERE pickup_id NOT BETWEEN 1 AND 265) invalid_pickup_id,count(*) FILTER(WHERE dropoff_id NOT BETWEEN 1 AND 265) invalid_dropoff_id,
    count(*) FILTER(WHERE pickup_id IN(264,265)) nonspatial_pickup_id,count(*) FILTER(WHERE dropoff_id IN(264,265)) nonspatial_dropoff_id,
    count(*) FILTER(WHERE miles=0) zero_distance,count(*) FILTER(WHERE miles<0) negative_distance,count(*) FILTER(WHERE miles IS NULL) null_distance,
    count(*) FILTER(WHERE base_fare<=0) nonpositive_fare,count(*) FILTER(WHERE base_fare IS NULL) null_fare,
    count(*) FILTER(WHERE duration_seconds<=0) nonpositive_duration,count(*) FILTER(WHERE duration_seconds>10800) over_three_hours,count(*) FILTER(WHERE duration_seconds IS NULL) AS unknown_duration,
    count(*) FILTER(WHERE reported_duration_seconds IS NOT NULL AND abs(reported_duration_seconds-duration_seconds)>1) reported_duration_disagrees,
    count(*) FILTER(WHERE strftime(pickup_at,'%Y-%m')!=${q(month)}) pickup_outside_file_month,
    count(*) FILTER(WHERE strftime(dropoff_at,'%Y-%m')!=${q(month)}) dropoff_outside_file_month FROM trips`))[0];
  assert.equal(profile.source_rows,profile.distinct_ingestion_rows);
  const distribution=await query(c,`SELECT strftime(pickup_at,'%Y-%m') pickup_month,strftime(dropoff_at,'%Y-%m') dropoff_month,count(*) trips FROM trips GROUP BY ALL ORDER BY ALL`);
  const anomalies=await query(c,`SELECT metric,event_at::DATE::VARCHAR date,hour(event_at)::INTEGER AS hour,${dstSql('event_at')} temporal_quality,count(*) trips FROM trips,LATERAL(VALUES ('pickup',pickup_at),('dropoff',dropoff_at))e(metric,event_at) WHERE event_at IS NOT NULL AND ${dstSql('event_at')}!='ordinary' GROUP BY ALL ORDER BY ALL`);
  const exclusions=await query(c,`SELECT metric,count(*) input_events,count(*) FILTER(WHERE event_at IS NULL) excluded_null_timestamp,
    count(*) FILTER(WHERE event_at IS NOT NULL AND (event_at<TIMESTAMP '${year}-03-01' OR event_at>=TIMESTAMP '${year}-06-01')) excluded_outside_approved_window,
    count(*) FILTER(WHERE event_at>=TIMESTAMP '${year}-03-01' AND event_at<TIMESTAMP '${year}-06-01') retained_events FROM trips,LATERAL(VALUES('pickup',pickup_at),('dropoff',dropoff_at))e(metric,event_at) GROUP BY metric ORDER BY metric`);
  await c.run(`CREATE OR REPLACE TEMP TABLE source_events AS ${eventSql(service,year)}`);
  const dir=path.join(staging,'sources',`${service}_${month}`);await mkdir(dir,{recursive:true});
  const eventsFile=path.join(dir,'zone_hour_activity.parquet');
  await c.run(`COPY (SELECT ${q(month)} source_month,* FROM source_events ORDER BY ALL) TO ${q(eventsFile)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
  const totals=await query(c,'SELECT metric,sum(trips) trips FROM source_events GROUP BY metric');
  for(const e of exclusions){assert.equal(Number(e.input_events),Number(e.excluded_null_timestamp)+Number(e.excluded_outside_approved_window)+Number(e.retained_events));assert.equal(Number(totals.find(r=>r.metric===e.metric)?.trips??0),Number(e.retained_events));}
  // One origin category per row, including explicit unknown endpoints. All CRZ measures
  // use exactly the same dropoff date/hour and verified official membership.
  await c.run(`CREATE OR REPLACE TEMP TABLE source_crz AS SELECT ${q(service)} service,${q(month)} source_month,dropoff_at::DATE::VARCHAR date,hour(dropoff_at)::INTEGER AS hour,
    CASE WHEN p."LocationID" IS NULL THEN CASE WHEN pickup_id IS NULL THEN 'missing' ELSE 'invalid' END WHEN p."LocationID">263 THEN 'nonspatial_'||p."LocationID"::VARCHAR ELSE p.borough END borough,
    CASE WHEN p.official_cbd_member=true THEN 'inside' WHEN p.official_cbd_member=false THEN 'outside' ELSE 'unknown' END origin_class,
    ${dstSql('dropoff_at')} temporal_quality,count(*)::BIGINT trips
    FROM trips LEFT JOIN zones p ON pickup_id=p."LocationID" JOIN zones d ON dropoff_id=d."LocationID" AND d.official_cbd_member=true
    WHERE dropoff_at>=TIMESTAMP '${year}-03-01' AND dropoff_at<TIMESTAMP '${year}-06-01' GROUP BY ALL`);
  const crzFile=path.join(dir,'crz_hour_origin.parquet');await c.run(`COPY (SELECT * FROM source_crz ORDER BY ALL) TO ${q(crzFile)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
  const crzTotals=await query(c,'SELECT origin_class,sum(trips) trips FROM source_crz GROUP BY origin_class ORDER BY origin_class');
  // Independent direct-record control: no dimension joins or endpoint-event SQL.
  const inside=(await query(c,'SELECT "LocationID" id FROM zones WHERE official_cbd_member=true')).map(r=>Number(r.id)).join(',');
  const directDaily=await query(c,`SELECT dropoff_at::DATE::VARCHAR date,count(*) ending,
    count(*) FILTER(WHERE pickup_id BETWEEN 1 AND 263 AND pickup_id NOT IN (${inside})) entering,
    count(*) FILTER(WHERE pickup_id IS NULL OR pickup_id NOT BETWEEN 1 AND 263) AS unknown
    FROM trips WHERE dropoff_at>=TIMESTAMP '${year}-03-01' AND dropoff_at<TIMESTAMP '${year}-06-01' AND dropoff_id IN (${inside}) GROUP BY ALL ORDER BY date`);
  const joinedDaily=await query(c,"SELECT date,sum(trips) ending,coalesce(sum(trips) FILTER(WHERE origin_class='outside'),0) entering,coalesce(sum(trips) FILTER(WHERE origin_class='unknown'),0) AS unknown FROM source_crz GROUP BY date ORDER BY date");
  assert.deepEqual(joinedDaily,directDaily,'Independent CRZ daily controls must match join-based cube');

  const boundary=await query(c,`SELECT metric,substr(date,1,7) event_month,sum(trips) trips FROM source_events WHERE substr(date,1,7)!=${q(month)} GROUP BY ALL ORDER BY ALL`);
  const manifest={...source,profile,event_month_distribution:distribution,event_exclusions:exclusions,dst_anomalies:anomalies,boundary_contributions:boundary,crz_totals:crzTotals,independent_crz_daily_controls:directDaily,global_trip_exclusions:0,duplicates_removed:0,identity:'canonical source + normalized physical row ordinal; no attribute deduplication',source_scan_count:1,events_file:path.relative(staging,eventsFile),crz_file:path.relative(staging,crzFile),output_rows:Number((await query(c,'SELECT count(*) n FROM source_events'))[0].n),coverage:'bounded February-May observed-source endpoint population; vendor completeness not certified'};
  await save(path.join(dir,'manifest.json'),manifest);
  await c.run('DROP TABLE source_events');await c.run('DROP TABLE source_crz');await c.run('DROP TABLE trips');
  console.log(JSON.stringify({service,month,source_rows:profile.source_rows,boundary,dst:anomalies}));
  return manifest;
}
