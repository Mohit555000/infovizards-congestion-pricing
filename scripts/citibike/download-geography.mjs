import {readFile, writeFile, mkdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
const manifest = JSON.parse(await readFile('data/geo/v1/source-manifest.json'));
const urls = {
  'zones.zip': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip',
  'lookup.csv': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
  'geofence.json': 'https://data.ny.gov/resource/srxy-5nxn.geojson?$limit=1000',
  'geofence-meta.json': 'https://data.ny.gov/api/views/srxy-5nxn.json',
  'cbd-meta.json': 'https://data.ny.gov/api/views/yfdc-w5jh.json',
  'cbd-zones.json': 'https://data.ny.gov/resource/yfdc-w5jh.json?$limit=1000',
  'cbd-overview.pdf': 'https://data.ny.gov/api/views/yfdc-w5jh/files/947678d0-5074-431d-8a84-a1409722b42e?download=true',
  'geofence-overview.pdf': 'https://data.ny.gov/api/views/srxy-5nxn/files/5c248307-890b-4af2-9203-ab15d134f534?download=true'
};
await mkdir('.cache/geo/official', {recursive: true});
for (const item of manifest.snapshot_files) {
  const url = urls[item.path.split('/').at(-1)];
  let bytes;
  try { bytes = await readFile(item.path); } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    const response = await fetch(url, {signal: AbortSignal.timeout(60000)});
    if (!response.ok) throw new Error(`${response.status}: ${url}`);
    bytes = Buffer.from(await response.arrayBuffer());
  }
  if (bytes.length > 5000000 || createHash('sha256').update(bytes).digest('hex') !== item.sha256) {
    throw new Error(`Source snapshot changed: ${url}. Review and version before replacing.`);
  }
  await writeFile(item.path, bytes);
}
execFileSync('/usr/bin/unzip', ['-tq', '.cache/geo/zones.zip']);
execFileSync('/usr/bin/unzip', ['-oq', '.cache/geo/zones.zip', '-d', '.cache/geo/official']);
console.log('Eight frozen official geography snapshots hash-verified; shapefile CRC verified.');
