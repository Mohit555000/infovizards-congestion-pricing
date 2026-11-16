import test from 'node:test';
import assert from 'node:assert/strict';
import {DuckDBInstance} from '@duckdb/node-api';
import {mkdir,rm,readFile,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {importLocal,hashFile} from '../src/tlc/source.mjs';
import {groupFor,geographyStatus} from '../src/tlc/geography.mjs';

test('Q5 review separates215ordinaryoutside from10specific unresolved cases',async()=>{
  const zones=JSON.parse(await readFile('data/geo/v1/zone-groups.json'));
  const g=geographyStatus(zones);
  assert.equal(g.verified_inside_ids.length,38);assert.equal(g.ordinary_outside_ids.length,215);
  assert.deepEqual(g.adjacency_unresolved_ids,[33,34,43,66,140,141,142,143,145,237]);
  assert.equal(groupFor(zones.find(z=>z.LocationID===1)),'outside');
  assert.equal(groupFor(zones.find(z=>z.LocationID===43)),'unresolved');
  assert.equal(groupFor(zones.find(z=>z.LocationID===140)),'unresolved');
  assert.equal(groupFor(zones.find(z=>z.LocationID===33)),'unresolved');
  assert.equal(groupFor(zones.find(z=>z.LocationID===264)),'nonspatial');
  assert.throws(()=>groupFor({LocationID:2,official_cbd_member:null}),/Unclassified/);
});

test('explicit local Parquet import preserves rows/timestamps and fails closed (SYNTHETIC software fixture)',async()=>{
  const dir=path.resolve('.cache/tlc/test-local-import');await mkdir(dir,{recursive:true});
  const db=await DuckDBInstance.create(':memory:',{autoinstall_known_extensions:'false',autoload_known_extensions:'false'}),c=await db.connect();
  const originalFetch=globalThis.fetch;globalThis.fetch=()=>{throw new Error('No network allowed in local import');};
  try{
    const file=path.join(dir,'fixture.parquet');
    await c.run(`COPY (SELECT TIMESTAMP '2026-03-31 23:59:59.123456' AS pickup_datetime,TIMESTAMP '2026-04-01 00:05:00.123456' AS dropoff_datetime,1::INTEGER AS PULocationID,4::INTEGER AS DOLocationID,1.2 AS trip_miles,1.0 AS base_passenger_fare,301::BIGINT AS trip_time) TO '${file}' (FORMAT PARQUET)`);
    const before=await hashFile(file),source=await importLocal(c,path.join(dir,'cache'),'fhvhv','2026-03',file);
    assert.equal(source.local_validation.row_count,1);assert.equal(source.local_validation.projected_row_count,1);assert.equal(source.local_validation.network_requests,0);
    assert.equal(source.etag,null);assert.equal(source.retrieved_at,null);assert.equal(source.raw_sha256,before);assert.equal(await hashFile(file),before);
    assert.ok(source.optional_unavailable.includes('cbd_congestion_fee'));
    const rows=(await c.runAndReadAll(`SELECT pickup_at::VARCHAR AS pu,dropoff_at::VARCHAR AS do_time FROM read_parquet('${source.file}')`)).getRowObjectsJson();
    assert.equal(rows[0].pu,'2026-03-31 23:59:59.123456');assert.equal(rows[0].do_time,'2026-04-01 00:05:00.123456');
    await assert.rejects(importLocal(c,path.join(dir,'cache'),'yellow','2026-03',file),/restricted/);
    await assert.rejects(importLocal(c,path.join(dir,'cache'),'fhvhv','2026-03',path.join(dir,'absent.parquet')),/ENOENT/);
    const bad=path.join(dir,'bad.parquet');await writeFile(bad,'not parquet');await assert.rejects(importLocal(c,path.join(dir,'other-cache'),'fhvhv','2026-03',bad));
  }finally{globalThis.fetch=originalFetch;c.closeSync();db.closeSync();await rm(dir,{recursive:true,force:true});}
});
