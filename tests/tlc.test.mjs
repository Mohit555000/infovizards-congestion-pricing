import test from 'node:test';
import {existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import { adapter,comparison,zoneStatus,allowedMonth } from '../src/tlc/methods.mjs';
import {DuckDBInstance} from '@duckdb/node-api';
import {readFile} from 'node:fs/promises';
test('TLC adapters require service-specific timestamp fields and tolerate unavailable fee',()=>{
 const common=['PULocationID','DOLocationID'];
 assert.equal(adapter('yellow',[...common,'tpep_pickup_datetime','tpep_dropoff_datetime']).fare,'fare_amount');
 assert.equal(adapter('fhvhv',[...common,'pickup_datetime','dropoff_datetime']).fare,'base_passenger_fare');
 assert.throws(()=>adapter('fhvhv',[...common,'tpep_pickup_datetime','tpep_dropoff_datetime']));
});
test('zone categories preserve264/265 and distinguish null/invalid',()=>{
 assert.equal(zoneStatus(264),'nonspatial');assert.equal(zoneStatus(265),'nonspatial');assert.equal(zoneStatus(null),'missing');assert.equal(zoneStatus(266),'invalid');assert.equal(zoneStatus(1),'spatial');
});
test('baseline ratios, threshold and partial/missing statuses are distinct',()=>{
 assert.equal(comparison(3000,6000,3).pct_change,100);assert.equal(comparison(2999,6000,3).comparison_status,'low_volume');assert.equal(comparison(0,1,1).comparison_status,'zero_baseline');
 assert.equal(comparison(null,1,1).comparison_status,'missing_baseline');assert.equal(comparison(1000,null,1).comparison_status,'missing_post_period');assert.equal(comparison(1000,2000,1,{complete:false}).comparison_status,'partial_coverage');
 assert.equal(comparison(1000,2000,1,{complete:false}).pct_change,null);assert.equal(comparison(999,2000,1).post_trips,2000);
 // Combined service threshold is evaluated after summing counts, not averaging percentages.
 assert.equal(comparison(500+500,1000+2000,1).pct_change,200);assert.throws(()=>allowedMonth('2026-06'));
});
test('DuckDB timestamp arithmetic retains midnight/month crossing and fractional time',async()=>{
 const db=await DuckDBInstance.create(':memory:');const c=await db.connect();try{
 const r=(await c.runAndReadAll("SELECT (TIMESTAMP '2024-03-31 23:59:59.5')::DATE::VARCHAR AS pu,(TIMESTAMP '2024-04-01 00:00:01.5')::DATE::VARCHAR AS do_date,epoch(TIMESTAMP '2024-04-01 00:00:01.5'-TIMESTAMP '2024-03-31 23:59:59.5') AS seconds")).getRowObjectsJson()[0];assert.equal(r.pu,'2024-03-31');assert.equal(r.do_date,'2024-04-01');assert.equal(r.seconds,2);
 }finally{c.closeSync();db.closeSync();}
});
test('actual TLC source reconciliation and retained multiplicity', {
  skip: !['manifest.json', 'yellow_2024-03/manifest.json'].some(file =>
    existsSync(new URL('../data/processed/tlc/' + file, import.meta.url))) && 'Run TLC preprocessing to generate source aggregates'
}, async()=>{
 const root=new URL('../data/processed/tlc/',import.meta.url);
 let current;try{current=JSON.parse(await readFile(new URL('manifest.json',root)));}catch{}
 if(current?.schema_version==='2.0.0'){
  const m=current.sources.find(s=>s.service==='yellow'&&s.month==='2024-03');
  assert.equal(m.global_trip_exclusions,0);assert.equal(m.duplicates_removed,0);assert.equal(m.source_scan_count,1);
  for(const e of m.event_exclusions)assert.equal(Number(e.input_events),Number(e.retained_events)+Number(e.excluded_null_timestamp)+Number(e.excluded_outside_approved_window));
  const db=await DuckDBInstance.create(':memory:'),c=await db.connect();try{
   const url=new URL(m.events_file,root);const{literal}=await import('../src/tlc/methods.mjs');
   const totals=(await c.runAndReadAll(`SELECT metric,sum(trips) trips FROM read_parquet(${literal(url.pathname)}) GROUP BY metric`)).getRowObjectsJson();
   for(const e of m.event_exclusions)assert.equal(Number(totals.find(r=>r.metric===e.metric).trips),Number(e.retained_events));
  }finally{c.closeSync();db.closeSync();}
 }else{
  const m=JSON.parse(await readFile(new URL('yellow_2024-03/manifest.json',root)));
  assert.equal(m.global_trip_exclusions,0);assert.equal(m.duplicates_removed,0);
  for(const e of m.event_exclusions)assert.equal(Number(e.input_events),Number(e.retained_events)+Number(e.excluded_null_timestamp)+Number(e.excluded_outside_requested_month));
 }
});
test('daily references preserve2024 and distinguish calendar from weekday semantics',async()=>{
 const{dailyReference}=await import('../src/tlc/methods.mjs');
 assert.equal(dailyReference('2025-05-31'),'2024-05-31');assert.equal(dailyReference('2026-05-31'),'2024-05-31');assert.equal(dailyReference('2025-05-31','weekday_364'),'2024-06-01');assert.throws(()=>dailyReference('2026-05-31','weekday_364'));
});


test('event month is selected by event timestamp and adjacent rows deduplicate by trip key',()=>{
 const trips=[{id:'x',pickup:'2024-03-31 23:59:00',dropoff:'2024-04-01 00:01:00',source:'March'}, {id:'y',pickup:'2024-04-01 00:01:00',dropoff:'2024-04-01 00:05:00',source:'April'}];
 const select=(metric,month)=>trips.filter(r=>r[metric].slice(0,7)===month);
 assert.equal(select('pickup','2024-03').length,1);assert.equal(select('dropoff','2024-03').length,0);assert.equal(select('dropoff','2024-04').length,2);
 const merged=[...select('dropoff','2024-04'),...select('dropoff','2024-04')];assert.equal(new Map(merged.map(r=>[r.id,r])).size,2);
});

test('CRZ entry and ending metrics share dropoff event date',()=>{
 const trips=[{pickup:'2024-03-31 23:55:00',dropoff:'2024-04-01 00:05:00',puInside:false,doInside:true},{pickup:'2024-04-01 00:01:00',dropoff:'2024-04-01 00:10:00',puInside:true,doInside:true}];
 const eventDate=t=>t.dropoff.slice(0,10);const entering=trips.filter(t=>!t.puInside&&t.doInside),ending=trips.filter(t=>t.doInside);
 assert.equal(eventDate(entering[0]),'2024-04-01');assert.equal(eventDate(ending[0]),eventDate(entering[0]));assert.equal(ending.length,2);
});

test('DST wall timestamps remain unchanged and anomaly is flagged, not shifted',async()=>{
 const db=await DuckDBInstance.create(':memory:');const c=await db.connect();try{const r=(await c.runAndReadAll("SELECT hour(TIMESTAMP '2024-03-10 02:15:00') AS wall_hour, (TIMESTAMP '2024-03-10 02:15:00')::VARCHAR AS preserved")).getRowObjectsJson()[0];assert.equal(Number(r.wall_hour),2);assert.equal(r.preserved,'2024-03-10 02:15:00');}finally{c.closeSync();db.closeSync();}
});
