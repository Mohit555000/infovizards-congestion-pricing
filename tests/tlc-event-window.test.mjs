import test from 'node:test';
import {existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import {DuckDBInstance} from '@duckdb/node-api';
import {mkdir,rm,readFile} from 'node:fs/promises';
import path from 'node:path';
import {eventSql,dstSql,registerSource,sourceTasks,literal as q} from '../src/tlc/methods.mjs';
import {processSource,query,regression} from '../src/tlc/processing.mjs';
import {generate} from '../src/tlc/outputs.mjs';
const root=process.cwd();
test('source registry rejects repeat ingestion but never deduplicates matching trip attributes',()=>{
 const r=new Map();registerSource(r,{service:'yellow',month:'2024-03',sha256:'one'});
 assert.throws(()=>registerSource(r,{service:'yellow',month:'2024-03',sha256:'one'}),/Duplicate ingestion/);
 assert.throws(()=>registerSource(r,{service:'yellow',month:'2024-04',sha256:'one'}),/content alias/);
 registerSource(r,{service:'yellow',month:'2024-04',sha256:'two'});assert.equal(r.size,2);assert.equal(sourceTasks().length,24);
});
test('production SQL preserves multiplicity, cross-month endpoints, DST and unknown counterpart (SYNTHETIC software fixture)',async()=>{
 const dir=path.join(root,'.cache/tlc/test-event-window');await mkdir(dir,{recursive:true});
 const db=await DuckDBInstance.create(':memory:',{memory_limit:'256MB'}),c=await db.connect();
 try{
  await c.run(`CREATE TABLE fixture AS SELECT * FROM (VALUES
   (TIMESTAMP '2024-03-31 23:59:59.5',TIMESTAMP '2024-04-01 00:01:00.5',1,4),
   (TIMESTAMP '2024-03-31 23:59:59.5',TIMESTAMP '2024-04-01 00:01:00.5',1,4),
   (TIMESTAMP '2024-03-10 01:55:00',TIMESTAMP '2024-03-10 02:15:00',1,4),
   (NULL,TIMESTAMP '2024-04-02 03:00:00',NULL,4),
   (TIMESTAMP '2024-05-31 23:55:00',TIMESTAMP '2024-06-01 00:05:00',1,4))t(pickup_at,dropoff_at,pickup_id,dropoff_id)`);
  await c.run('ALTER TABLE fixture ADD COLUMN miles DOUBLE');await c.run('ALTER TABLE fixture ADD COLUMN base_fare DOUBLE');await c.run('ALTER TABLE fixture ADD COLUMN reported_duration_seconds BIGINT');await c.run('ALTER TABLE fixture ADD COLUMN cbd_congestion_fee DOUBLE');
  const f=path.join(dir,'fixture.parquet');await c.run(`COPY fixture TO ${q(f)} (FORMAT PARQUET)`);
  await c.run(`CREATE TABLE zones AS SELECT * FROM read_json_auto(${q(path.join(root,'data/geo/v1/zone-groups.json'))})`);
  const m=await processSource(c,{service:'yellow',month:'2024-03',file:f,sha256:'SYNTHETIC'},dir);
  const events=await query(c,`SELECT * FROM read_parquet(${q(path.join(dir,m.events_file))})`);
  assert.equal(events.filter(r=>r.metric==='dropoff'&&r.date==='2024-04-01').reduce((n,r)=>n+Number(r.trips),0),2);
  assert.equal(events.filter(r=>r.metric==='dropoff'&&r.temporal_quality==='spring_gap').reduce((n,r)=>n+Number(r.trips),0),1);
  assert.equal(events.filter(r=>r.metric==='pickup').reduce((n,r)=>n+Number(r.trips),0),4);
  const crz=await query(c,`SELECT * FROM read_parquet(${q(path.join(dir,m.crz_file))})`);
  assert.equal(crz.filter(r=>r.date==='2024-04-01'&&r.origin_class==='outside').reduce((n,r)=>n+Number(r.trips),0),2);
  assert.equal(crz.filter(r=>r.origin_class==='unknown').reduce((n,r)=>n+Number(r.trips),0),1);
  assert.equal(m.duplicates_removed,0);assert.equal(m.profile.source_rows,'5');
  const preserved=await query(c,`SELECT pickup_at::VARCHAR ts FROM fixture WHERE pickup_at IS NOT NULL ORDER BY ts LIMIT 1`);assert.equal(preserved[0].ts,'2024-03-10 01:55:00');
  const flags=await query(c,`SELECT ${dstSql("TIMESTAMP '2024-11-03 01:30:00'")} flag`);assert.equal(flags[0].flag,'fall_fold');
  const zones=JSON.parse(await readFile(path.join(root,'data/geo/v1/zone-groups.json'),'utf8'));
  const products=await generate(c,dir,[m],zones,'SYNTHETIC',path.join(root,'data/processed/tlc'));
  assert.ok(products.outputs.q7_tlc_origin_borough.some(r=>r.borough==='EWR'),'Preserve official lookup EWR category');
  assert.ok(products.outputs.q4_tlc_zone_change.every(r=>r.pct_change===null&&r.base_trips===null));
  assert.ok(products.outputs.q2_tlc_hour_profile.every(r=>r.avg_trips_entering_per_day===null));
 }finally{c.closeSync();db.closeSync();await rm(dir,{recursive:true,force:true});}
});
test('real cached March2024 regression assigns all631April dropoffs with production SQL', {
  skip: !existsSync(path.join(root, '.cache/tlc/yellow_2024-03/normalized.parquet')) && 'Populate the March 2024 TLC cache to run this check'
}, async()=>{
 const db=await DuckDBInstance.create(':memory:'),c=await db.connect();try{const r=await regression(c,path.join(root,'.cache/tlc'));assert.equal(r.actual_april_dropoffs,631);}finally{c.closeSync();db.closeSync();}
});
