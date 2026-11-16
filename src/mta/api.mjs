import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

export const RESOURCE = 'https://data.ny.gov/resource/t6yz-b64h.json';
export const METADATA = 'https://data.ny.gov/api/views/t6yz-b64h.json';
export const digest = (s) => createHash('sha256').update(s).digest('hex');

export class SocrataClient {
  constructor({ cacheDir, fetchFn = fetch, pageSize = 5000, retries = 3, maxBytes = 12_000_000,
    sleep = (ms) => new Promise((r) => setTimeout(r, ms)), offline = false, onRequest = () => {} } = {}) {
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 10000) throw new Error('pageSize must be 1..10000');
    Object.assign(this, { cacheDir, fetchFn, pageSize, retries, maxBytes, sleep, offline, onRequest });
    this.log = [];
  }
  async get(url, { fresh = false } = {}) {
    const key = digest(url);
    const file = path.join(this.cacheDir, `${key}.json`);
    if (!fresh) {
      try {
        const cached = JSON.parse(await readFile(file, 'utf8'));
        if (cached.url !== url || digest(cached.body) !== cached.sha256) throw new Error('Cache integrity failure');
        this.log.push({ ...cached, body: undefined, cache_hit: true });
        return JSON.parse(cached.body);
      } catch (e) { if (e.code !== 'ENOENT') throw e; }
    }
    if (this.offline) throw new Error(`Offline cache miss: ${url}`);
    let last;
    for (let attempt = 0; attempt <= this.retries; attempt++) {
      try {
        this.onRequest(url, attempt);
        const response = await this.fetchFn(url, { signal: AbortSignal.timeout(90000), headers: { Accept: 'application/json' } });
        if (!response.ok) {
          const error = new Error(`HTTP ${response.status}: ${url}`);
          error.retryable = response.status === 429 || response.status >= 500;
          error.retryAfter = Math.min(60000, Math.max(0, Number(response.headers.get('retry-after')) * 1000 || 0));
          await response.body?.cancel();
          throw error;
        }
        const chunks = []; let size = 0;
        for await (const chunk of response.body) {
          size += chunk.length;
          if (size > this.maxBytes) throw Object.assign(new Error('Response byte cap exceeded'), { retryable: false });
          chunks.push(Buffer.from(chunk));
        }
        const body = Buffer.concat(chunks).toString('utf8');
        const data = JSON.parse(body);
        const record = { url, retrieved_at: new Date().toISOString(), bytes: size, sha256: digest(body),
          etag: response.headers.get('etag'), body };
        await mkdir(this.cacheDir, { recursive: true });
        // Atomic replacement; URL + source revision namespace makes cache scope explicit.
        const temp = `${file}.${process.pid}.tmp`;
        await writeFile(temp, JSON.stringify(record));
        const { rename } = await import('node:fs/promises');
        await rename(temp, file);
        this.log.push({ ...record, body: undefined, cache_hit: false });
        return data;
      } catch (e) {
        last = e;
        if (e.retryable === false || attempt === this.retries) throw e;
        await this.sleep(e.retryAfter || 500 * 2 ** attempt);
      }
    }
    throw last;
  }
  async query(params, options) {
    return this.get(`${RESOURCE}?${new URLSearchParams(params)}`, options);
  }
  async pages(params) {
    if (!params.$order) throw new Error('Pagination requires deterministic total ordering');
    const all = []; const seen = new Set();
    for (let offset = 0; ; offset += this.pageSize) {
      const rows = await this.query({ ...params, $limit: String(this.pageSize), $offset: String(offset) });
      if (!Array.isArray(rows)) throw new Error('Expected API row array');
      if (rows.length > this.pageSize) throw new Error('API page exceeds requested limit');
      if (rows.length) {
        const fingerprint = digest(JSON.stringify(rows));
        if (seen.has(fingerprint)) throw new Error('Repeated API page: pagination failed');
        seen.add(fingerprint);
      }
      all.push(...rows);
      if (all.length > 200000) throw new Error('Query row cap exceeded; partition the range');
      if (rows.length < this.pageSize) return all;
    }
  }
}
