import {existsSync, readdirSync} from 'node:fs';
import {exclusionSql} from '../src/citibike/cleaning.mjs';
import test from'node:test';import assert from'node:assert/strict';import{readFile}from'node:fs/promises';import{geoArea,geoPath,geoMercator}from'd3';import{DuckDBInstance}from'@duckdb/node-api';import{stationChange,coordValid}from'../src/citibike/methods.mjs';
test('official shape IDs, lookup gaps and MTA membership verified',async()=>{const g=JSON.parse(await readFile('data/geo/v1/taxi-zones.geojson'));assert.equal(g.features.length,263);assert.equal(new Set(g.features.map(f=>f.id)).size,263);const m=JSON.parse(await readFile('data/geo/v1/zone-groups.json'));assert.equal(m.length,265);assert.equal(m.filter(r=>r.official_cbd_member===true).length,38);assert.equal(m.find(r=>r.LocationID===43).zone_group,'ambiguous_central_park');assert.ok(m.filter(r=>r.LocationID>263).every(r=>r.classification_status==='no_polygon'));});
test('D3-specific winding produces local spherical areas and finite path geometry (not a visual test)',async()=>{const g=JSON.parse(await readFile('data/geo/v1/taxi-zones-d3.geojson'));const p=geoPath(geoMercator().fitSize([800,800],g));for(const f of g.features){assert.ok(geoArea(f)<0.1);assert.ok(p(f)&&!/NaN|Infinity/.test(p(f)));}});
const extensionDirectory = '.cache/tlc/extensions';
const spatialInstalled = existsSync(extensionDirectory)
  && readdirSync(extensionDirectory, {recursive: true}).some(file => file.endsWith('/spatial.duckdb_extension'));
const cityBikeCacheReady = [2024, 2025, 2026].every(year =>
  ['clean', 'coordinate-matches'].every(file => existsSync(`.cache/citibike/${year}-${file}.parquet`))
);
test('boundary joins retain two candidates rather than arbitrary zone assignment', {skip: !spatialInstalled && 'Install the DuckDB spatial extension to run this check'}, async()=>{const db=await DuckDBInstance.create(':memory:',{extension_directory:process.cwd()+'/.cache/tlc/extensions',autoinstall_known_extensions:'false'});const c=await db.connect();try{await c.run('LOAD spatial');const r=(await c.runAndReadAll("SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),ST_Point(1,0.5))AS interior,ST_Covers(ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),ST_Point(1,0.5))AS boundary")).getRowObjectsJson()[0];assert.equal(r.interior,false);assert.equal(r.boundary,true);const candidates=(await c.runAndReadAll("SELECT count(*) AS n FROM (VALUES ('POLYGON((0 0,1 0,1 1,0 1,0 0))'),('POLYGON((1 0,2 0,2 1,1 1,1 0))')) z(wkt) WHERE ST_Covers(ST_GeomFromText(wkt),ST_Point(1,0.5))")).getRowObjectsJson();assert.equal(Number(candidates[0].n),2);}finally{c.closeSync();db.closeSync();}});
test('coordinates and station baseline states are distinct',()=>{assert.equal(coordValid(-74,40.7),true);assert.equal(coordValid(null,40),false);assert.equal(coordValid(181,40),false);assert.equal(stationChange(null,200).pct_change,null);assert.equal(stationChange(null,200).new_station_confirmed,null);assert.equal(stationChange(0,200).comparison_status,'zero_baseline');assert.equal(stationChange(99,200).pct_change,null);assert.equal(stationChange(100,200).pct_change,100);assert.equal(stationChange(100,200,{identityValid:false}).comparison_status,'uncertain_station_identity');});
test('real Q6 totals, joint composition shares, group ratios and station keys reconcile',async()=>{const base='data/processed/citibike/';const manifest=JSON.parse(await readFile(base+'manifest.json'));const mix=JSON.parse(await readFile(base+'q6_citibike_group_mix.json'));for(const run of manifest.runs.filter(r=>r.retained!==undefined)){const selected=mix.filter(r=>r.year===run.year);assert.equal(selected.reduce((n,r)=>n+Number(r.rides),0),run.retained);for(const group of new Set(selected.map(r=>r.zone_group)))assert.ok(Math.abs(selected.filter(r=>r.zone_group===group).reduce((n,r)=>n+r.share_of_group_pct,0)-100)<1e-8);assert.equal(run.raw_rows,run.exact_duplicates_removed+run.exclusions.reduce((n,r)=>n+Number(r.rides),0));}const growth=JSON.parse(await readFile(base+'q6_citibike_group_growth.json'));for(const r of growth)if(r.pct_change!==null)assert.ok(Math.abs(r.pct_change-100*(Number(r.post_rides)/Number(r.base_rides)-1))<1e-8);const stations=JSON.parse(await readFile(base+'q6_citibike_station_change.json'));assert.equal(new Set(stations.map(r=>JSON.stringify([r.post_year,r.station_id]))).size,stations.length);for(const r of stations){assert.equal(typeof r.station_id,'string');if(r.pct_change!==null){assert.ok(r.base_rides>=100);assert.equal(r.comparison_status,'valid');}}});

test('real cleaned rows and coordinate audit preserve valid counts without arbitrary spatial matches', {skip: !cityBikeCacheReady && 'Run Citi Bike preprocessing to generate the cleaned rows and coordinate audit'}, async()=>{
 const db=await DuckDBInstance.create(':memory:',{memory_limit:'512MB',threads:'2',autoinstall_known_extensions:'false'});const c=await db.connect();
 try {for(const year of [2024,2025,2026]){
  const r=(await c.runAndReadAll(`SELECT count(*) AS n,count(*)FILTER(WHERE start_at IS NULL OR end_at<=start_at OR strftime(start_at,'%Y-%m')!='${year}-05') AS invalid,count(distinct ride_id) AS ids FROM read_parquet('.cache/citibike/${year}-clean.parquet')`)).getRowObjectsJson()[0];
  const log=JSON.parse(await readFile(`data/processed/citibike/${year}-log.json`));assert.equal(Number(r.n),log.retained);assert.equal(Number(r.ids),log.retained);assert.equal(Number(r.invalid),0);assert.equal(Number(log.coverage.observed_start_dates),31);
  const a=(await c.runAndReadAll(`SELECT sum(rides) AS n,count(*)FILTER(WHERE n_matches!=1 AND zone_id IS NOT NULL) AS bad FROM read_parquet('.cache/citibike/${year}-coordinate-matches.parquet')`)).getRowObjectsJson()[0];assert.equal(Number(a.n),log.retained);assert.equal(Number(a.bad),0);
 }}finally{c.closeSync();db.closeSync();}
});

test('controlled cleaning fixture covers exclusion precedence, duration tails and cross-month endings',async()=>{
 const db=await DuckDBInstance.create(':memory:');const c=await db.connect();
 try{
 const r=(await c.runAndReadAll(`WITH raw(ride_id,id_versions,s,e) AS(VALUES
 ('short',1,'2024-05-01 00:00:00.123','2024-05-01 00:00:01.123'),
 ('long',1,'2024-05-31 23:00:00','2024-06-01 04:00:00'),
 ('zero',1,'2024-05-01','2024-05-01'),
 ('outside',1,'2024-04-30 23:59:59','2024-05-01'),
 ('bad',1,'invalid','2024-05-01'),
 ('conflict',2,'2024-05-01','2024-05-02'),
 (NULL,2,'invalid','invalid')),
 checked AS(SELECT *,try_cast(s AS TIMESTAMP) AS start_at,try_cast(e AS TIMESTAMP) AS end_at FROM raw)
 SELECT ride_id,${exclusionSql(2024)} AS reason FROM checked`)).getRowObjectsJson();
 assert.deepEqual(r.map(x=>x.reason),[null,null,'nonpositive_duration','outside_window','invalid_timestamp','conflicting_ride_id','missing_ride_id']);
 assert.throws(()=>exclusionSql(2027),/Unapproved/);
 }finally{c.closeSync();db.closeSync();}
});
