import {createHash} from 'node:crypto';
import {createReadStream} from 'node:fs';
import {mkdir,readFile,writeFile,stat,rename,rm} from 'node:fs/promises';
import path from 'node:path';
import {adapter,literal as q} from './methods.mjs';
export async function hashFile(file) {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(file)) hash.update(chunk);
  return hash.digest('hex');
}
export const save = (file,data) => writeFile(file,JSON.stringify(data,null,2)+'\n');
export async function acquire(c, cache, service, month, offline) {
  const dir=path.join(cache,`${service}_${month}`), file=path.join(dir,'normalized.parquet'), metadata=path.join(dir,'source.json');
  await mkdir(dir,{recursive:true});
  const url=`https://d37ci6vzurychx.cloudfront.net/trip-data/${service}_tripdata_${month}.parquet`;
  let info;
  try { info=JSON.parse(await readFile(metadata,'utf8')); await stat(file); } catch { info=null; }
  if (info) {
    if (info.url!==url || info.service!==service || info.month!==month) throw new Error('Cached source identity mismatch');
    const sha256=await hashFile(file);
    if (info.normalized_sha256 && sha256!==info.normalized_sha256) throw new Error(`Cache hash mismatch ${file}`);
    // Freeze legacy complete caches on first use; source metadata/ETag already exists.
    if (!info.normalized_sha256) { info.normalized_sha256=sha256; await save(metadata,info); }
    return {...info,file,sha256,cache_reused:true};
  }
  if (offline) throw new Error(`Offline cache miss ${service} ${month}`);
  let lastError;
  for (let attempt=1;attempt<=3;attempt++) {
    const partial=path.join(dir,'normalized.partial.parquet');
    try {
      const head=await fetch(url,{method:'HEAD',signal:AbortSignal.timeout(60000)});
      if (!head.ok) throw new Error(`HEAD ${head.status} ${url}`);
      const schema=(await c.runAndReadAll(`DESCRIBE SELECT * FROM read_parquet(${q(url)})`)).getRowObjectsJson();
      const names=schema.map(r=>r.column_name), a=adapter(service,names);
      for (const name of [a.pickup,a.dropoff]) if(schema.find(r=>r.column_name===name).column_type!=='TIMESTAMP') throw new Error(`Unexpected timestamp type ${name}`);
      const optional=name=>names.includes(name)?`"${name}"`:'NULL';
      const sql=`COPY (SELECT "${a.pickup}" AS pickup_at,"${a.dropoff}" AS dropoff_at,PULocationID AS pickup_id,DOLocationID AS dropoff_id,${optional(a.miles)} AS miles,${optional(a.fare)} AS base_fare,${optional(service==='fhvhv'?'trip_time':'__unavailable')} AS reported_duration_seconds,${optional('cbd_congestion_fee')} AS cbd_congestion_fee FROM read_parquet(${q(url)})) TO ${q(partial)} (FORMAT PARQUET, COMPRESSION ZSTD)`;
      await rm(partial,{force:true});
      console.log(`Projecting ${service} ${month}, attempt ${attempt}/3`);
      await c.run(sql);
      const after=await fetch(url,{method:'HEAD',signal:AbortSignal.timeout(60000)});
      if (!after.ok || after.headers.get('etag')!==head.headers.get('etag') || after.headers.get('content-length')!==head.headers.get('content-length')) throw new Error('Source revision changed during projection');
      if ((await stat(file).catch(()=>null))) await rename(file,path.join(dir,`unverified-previous-${Date.now()}.parquet`));
      await rename(partial,file);
      info={service,month,url,etag:head.headers.get('etag'),source_bytes:Number(head.headers.get('content-length')),retrieved_at:new Date().toISOString(),schema,adapter:a,projection_sql:sql,normalized_sha256:await hashFile(file),optional_unavailable:[a.miles,a.fare,'cbd_congestion_fee',...(service==='yellow'?['trip_time']:[])].filter(n=>!names.includes(n))};
      await save(metadata,info);
      return {...info,file,sha256:info.normalized_sha256,cache_reused:false};
    } catch(error) {
      lastError=error; console.error(`Source failure ${service} ${month}, attempt ${attempt}/3: ${error.message}`);
      if(attempt<3) await new Promise(resolve=>setTimeout(resolve,attempt*15000));
    }
  }
  throw lastError;
}

// Explicit local import only: never calls acquire/fetch or installs extensions.
export async function importLocal(c, cache, service, month, localPath) {
  if (service !== 'fhvhv' || month !== '2026-03') throw new Error('Local completion is restricted to missing HVFHV March2026');
  const input = path.resolve(localPath), before = await stat(input), rawHash = await hashFile(input);
  const schema = (await c.runAndReadAll(`DESCRIBE SELECT * FROM read_parquet(${q(input)})`)).getRowObjectsJson();
  const names = schema.map(r => r.column_name), a = adapter(service, names);
  for (const name of [a.pickup, a.dropoff]) if (schema.find(r => r.column_name === name).column_type !== 'TIMESTAMP') throw new Error(`Unexpected timestamp type ${name}`);
  for (const name of ['PULocationID', 'DOLocationID']) if (schema.find(r => r.column_name === name).column_type !== 'INTEGER') throw new Error(`Unexpected zone ID type ${name}`);
  const rowCount = Number((await c.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${q(input)})`)).getRowObjectsJson()[0].n);
  if (!Number.isSafeInteger(rowCount) || rowCount <= 0) throw new Error('Local Parquet must contain a safe positive row count');
  const dir = path.join(cache, `${service}_${month}`), file = path.join(dir, 'normalized.parquet'), metadata = path.join(dir, 'source.json');
  await mkdir(dir, {recursive: true});
  // Existing complete normalized inputs are immutable; only reuse exact provenance.
  let existing;
  try { existing = JSON.parse(await readFile(metadata, 'utf8')); } catch(error) { if (error.code !== 'ENOENT') throw error; }
  if (existing) {
    if (existing.raw_sha256 !== rawHash) throw new Error('Existing local source differs; refusing replacement');
    return acquire(c, cache, service, month, true);
  }
  const partial = path.join(dir, 'local-normalized.partial.parquet');
  const optional = name => names.includes(name) ? `"${name}"` : 'NULL';
  const sql = `COPY (SELECT "${a.pickup}" AS pickup_at,"${a.dropoff}" AS dropoff_at,PULocationID AS pickup_id,DOLocationID AS dropoff_id,${optional(a.miles)} AS miles,${optional(a.fare)} AS base_fare,${optional('trip_time')} AS reported_duration_seconds,${optional('cbd_congestion_fee')} AS cbd_congestion_fee FROM read_parquet(${q(input)})) TO ${q(partial)} (FORMAT PARQUET, COMPRESSION ZSTD)`;
  await rm(partial, {force: true});
  await c.run(sql);
  const projectedCount = Number((await c.runAndReadAll(`SELECT count(*) AS n FROM read_parquet(${q(partial)})`)).getRowObjectsJson()[0].n);
  const after = await stat(input);
  if (projectedCount !== rowCount || after.size !== before.size || await hashFile(input) !== rawHash) throw new Error('Local source/projection integrity failed');
  if (await stat(file).catch(() => null)) await rename(file, path.join(dir, `unverified-previous-${Date.now()}.parquet`));
  await rename(partial, file);
  const info = {service, month, url: `https://d37ci6vzurychx.cloudfront.net/trip-data/${service}_tripdata_${month}.parquet`,
    acquisition: 'user_supplied_local_file', local_path: input, source_bytes: before.size, raw_sha256: rawHash,
    etag: null, retrieved_at: null, imported_at: new Date().toISOString(), schema, adapter: a, projection_sql: sql,
    local_validation: {row_count: rowCount, projected_row_count: projectedCount, timestamp_type: 'TIMESTAMP', network_requests: 0},
    normalized_sha256: await hashFile(file), optional_unavailable: [a.miles, a.fare, 'trip_time', 'cbd_congestion_fee'].filter(n => !names.includes(n))};
  await save(metadata, info);
  return {...info, file, sha256: info.normalized_sha256, cache_reused: false};
}
