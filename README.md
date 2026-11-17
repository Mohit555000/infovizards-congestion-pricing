# NYC Congestion Pricing: Changes in Urban Mobility

**Team InfoVizards** · Swati Singh, Saakshi Narayan, and Mohit Gangwani

NYU Information Visualization (CS-GY 6313 B), Fall 2026

**[Explore the interactive Observable notebook](https://observablehq.com/@swati-singh/nyc-congestion-pricing-infovizards)**

## Project Overview

New York City began tolling vehicles entering Manhattan's Congestion Relief Zone (CRZ) on January 5, 2025. The zone covers local streets at or below 60th Street, with exclusions for the FDR Drive, West Side Highway/Route 9A, and the Hugh L. Carey Tunnel connections to West Street. [MTA program overview](https://www.mta.info/document/131571)

We examine how recorded vehicle entries, taxi and ride-hail activity, and Citi Bike trips changed around the zone. The charts show when and where those changes occurred and which patterns merit closer study.

## Research Questions

| Question | What the visualization investigates |
| --- | --- |
| Q1 · Daily entries | How did MTA entries and TLC trips entering the CRZ change over time? |
| Q2 · Hourly patterns | How do entries vary by weekday and hour, including around peak and overnight toll boundaries? |
| Q3 · Crossings and vehicle classes | How did weekly entries change by crossing and vehicle class relative to the first four complete launch weeks? |
| Q4 · Taxi zones | Which zones gained or lost yellow taxi and ride-hail pickups and drop-offs relative to 2024? |
| Q5 · Outside the zone | How did trip endings inside the CRZ compare with endings elsewhere? Where do uncertain boundary and adjacency classifications limit that comparison? |
| Q6 · Citi Bike | How did May ride starts, bike type, and rider composition change across geographic groups and stations? |
| Q7 · Detection regions and pickup boroughs | Which detection regions and recorded pickup boroughs show changes worth investigating? |

## Data Sources

| Source | Processed coverage |
| --- | --- |
| [MTA CRZ vehicle entries](https://data.ny.gov/d/t6yz-b64h) | January 5, 2025–November 7, 2026. First-detected entries by crossing, detection region, vehicle class, and time. Excluded-roadway entries are kept separate. |
| [NYC TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | March–May events in 2024, 2025, and 2026. The pipeline uses February–May Parquet files for both yellow taxi and high-volume for-hire vehicles (HVFHV), 24 files in all. |
| [Citi Bike trip histories](https://citibikenyc.com/system-data) | NYC ride starts in May 2024, May 2025, and May 2026. Each monthly archive contains multiple CSVs. |
| [TLC taxi zones](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/8meu-9t5y/about_data), [MTA CBD taxi zones](https://data.ny.gov/d/yfdc-w5jh), and [MTA advisory geofence](https://data.ny.gov/d/srxy-5nxn) | The release includes six original geography snapshots and the derived polygons and zone lookup. The source manifest records their content hashes. |

## Data Preprocessing and Methodology

- **Tools and ingestion.** Node.js and npm run the pipeline. DuckDB reads Parquet and CSV, performs spatial joins, and computes aggregates. The MTA extractor pages through bounded Socrata queries and checks the dataset revision. The Citi Bike downloader validates archive integrity and inventories every CSV. D3 renders the charts in Observable.
- **TLC event dates.** A pickup belongs to its recorded pickup month. A drop-off, including a trip ending in or entering the CRZ, belongs to its recorded drop-off month. Reading the adjacent source months captures cross-month records. The pipeline prevents rereading an identical source and retains separate records with identical trip attributes. It flags ambiguous or nonexistent local times around clock changes. The primary hourly profile excludes those flagged records and tracks exposure.
- **Geography.** The MTA list identifies 38 CRZ taxi zones. A TLC entering trip must have a known pickup outside that set and a drop-off inside it. Missing, invalid, and nonspatial zone IDs remain separate. The taxi-zone approach approximates the road boundary. Q5 keeps ten unresolved boundary or adjacency zones in their own group. Citi Bike starts use spatial containment and retain ambiguous or unmatched points.
- **Aggregation and comparison.** MTA tables summarize daily, hourly, weekly, and monthly entries. Q1's MTA comparison uses a 364-day weekday alignment. Q2 and Q7 compare supported calendar months across 2025 and 2026. Q3 indexes weekly entries to the first four complete launch weeks. TLC uses matched periods in 2024 for both later years. Citi Bike compares each later May with May 2024. Service and group percentages use the combined counts before division.
- **Cleaning and missing data.** Citi Bike cleaning removes exact duplicate rows and quarantines conflicting ride IDs. It also excludes missing ride IDs, invalid timestamps, nonpositive durations, and starts outside May. Missing or incomplete comparisons and zero denominators remain unavailable. Q4 suppresses percentage changes for zones with fewer than 1,000 baseline trips per month on average. Q6 suppresses station percentages below 100 baseline rides and requires a same-ID, same-zone match within the study's 100-metre stability screen. Coverage, counts, and control totals are checked before outputs are accepted.

## Interactive Visualizations

[Open the finished Observable notebook](https://observablehq.com/@swati-singh/nyc-congestion-pricing-infovizards). Brush or select a date range in Q1 to update the whole weeks and months available in subsequent charts. Use the controls to switch years, taxi services, event types, and measurement units. The heatmap and hourly profiles show timing, the crossing panels show weekly trends, and the maps and bar charts show geographic differences. Hover over or select a mark to inspect its counts and comparison period.

## Key Findings

- **Daily traffic and timing.** From March through May, recorded MTA entries averaged 502,623 per day in 2025 and 486,607 in 2026, a decline of 3.2%. In March 2026, average weekday entries rose from 9,028 in the hour before the 5 a.m. peak boundary to 14,402 in the hour after it. The hourly counts show timing patterns across the boundary.
- **Crossings.** In the week of October 26–November 1, 2026, Brooklyn Bridge had the largest absolute index change among cars, pickups, and vans, with an index of 78.8 against its launch baseline of 100. Lincoln Tunnel bus entries indexed at 163.1 for that week.
- **Taxi zones.** In March–May 2026 compared with the same months of 2024, Corona zone 57 had the largest valid combined drop-off gain among mapped zones, up 36.9%. Across the same period, combined drop-offs fell 4.1% inside the CRZ and rose 10.9% in other outside zones. The ten unresolved zones rose 3.4% as a separate group.
- **Citi Bike.** Recorded May ride starts inside the zone rose from 1,948,489 in 2024 to 2,150,097 in 2026, up 10.3%. The electric bike share inside the zone rose from 59.8% to 69.6%, and the casual rider share fell from 18.6% to 17.1%.
- **Detection regions and pickup boroughs.** Brooklyn led the MTA detection regions in May 2026 at 118,830 entries per day. New Jersey had the largest drop from May 2025, down 7.0%. In the separate TLC comparison, March–May 2026 trips entering the zone from recorded Manhattan pickups outside it fell 4.3% versus 2024. Brooklyn pickup trips rose 2.4%.

## Limitations

The MTA series begins at launch. TLC covers three months per year, and Citi Bike covers May only. Provider reporting, detector issues, changing station coverage, and seasonality affect the comparisons. The north and across-river TLC subgroup comparisons remain unresolved. Citi Bike adjacency labels are provisional, and station IDs and coordinates cannot establish historical identity or opening dates. MTA detection regions locate where entries were recorded, and TLC pickup boroughs locate recorded trip starts. The datasets use different measures and cannot identify individual mode shifts or isolate the effect of the toll.

## Repository Structure

| Path | Contents |
| --- | --- |
| `src/` and `scripts/` | Ingestion, transformations, spatial joins, processing commands, and validators |
| `data/source/geography/` and `data/geo/v1/` | Original geography snapshots and derived geometry and lookups |
| `data/processed/` | Frozen analytical outputs with source and validation metadata |
| `notebooks/files/` | Exact 16 CSV, JSON, and GeoJSON attachments used by the published notebook |
| `data/observable-attachments.json` | Attachment names, corresponding analytical files, and SHA-256 checksums for Observable revision 178 |
| `tests/` | Preprocessing and validation tests |

## Reproducing the Analysis

Use Node.js 24 LTS, npm 11 or newer, and `/usr/bin/unzip` for geography and Citi Bike. Run commands from the repository root. Source acquisition requires network access and several gigabytes of disk space.

### Verify the release

```sh
npm ci
npm run check
npm run check:mta
```

These commands check the committed files and run the preprocessing tests. Tests that require a raw cache, regenerated intermediates, or the DuckDB spatial extension report a skip when those inputs are absent. The committed artifacts capture the source revisions used in the notebook. Regeneration may encounter revised source data, so keep the frozen outputs when reproducing the published charts.

### Set up DuckDB extensions for a regeneration run

```sh
node --input-type=module <<'NODE'
import {mkdir} from 'node:fs/promises';
import {resolve} from 'node:path';
import {DuckDBInstance} from '@duckdb/node-api';

const extensionDirectory = resolve('.cache/tlc/extensions');
await mkdir(extensionDirectory, {recursive: true});
const db = await DuckDBInstance.create(':memory:', {extension_directory: extensionDirectory});
const conn = await db.connect();
try {
  for (const extension of ['httpfs', 'spatial']) {
    await conn.run("INSTALL " + extension + " FROM 'https://extensions.duckdb.org'");
    await conn.run('LOAD ' + extension);
  }
} finally {
  conn.closeSync();
  db.closeSync();
}
NODE
```

### Geography, then MTA, TLC, and Citi Bike

```sh
mkdir -p .cache/geo
cp data/source/geography/* .cache/geo/
node scripts/citibike/download-geography.mjs
npm run process:geography

npm run process:mta -- --start 2025-01-05 --end 2026-11-07
npm run check:mta

node scripts/tlc/prepare-sources.mjs
npm run process:tlc -- --all
node scripts/validate/tlc-outputs.mjs

npm run download:citibike
npm run process:citibike
npm test
```

The geography downloader checks the included snapshots and retrieves the two official reference PDFs before unpacking the shape archive. MTA processing queries the official dataset for the fixed date range. Once that exact cache is populated, add `--offline` to replay the same MTA or TLC source requests without fetching again.

The 24 TLC trip files are retrieved from the TLC source page or the official `https://d37ci6vzurychx.cloudfront.net/trip-data/` endpoint as `yellow_tripdata_YYYY-MM.parquet` and `fhvhv_tripdata_YYYY-MM.parquet`, for February through May in each of 2024, 2025, and 2026. The large Parquet inputs are external. The bootstrap command prepares March 2024 yellow taxi data for the cross-month regression, then the pipeline acquires the remaining files. If the DuckDB route cannot acquire the March 2026 HVFHV file, download the official file through TLC and run `node scripts/tlc/prepare-sources.mjs --local-march-2026 /path/to/fhvhv_tripdata_2026-03.parquet` before the full run. Run the TLC output validator after regeneration, when the per-source aggregates it checks are available.

The large Citi Bike archives are external. The source policy permits use in noncommercial studies and restricts redistribution as a standalone dataset. The downloader retrieves [May 2024](https://s3.amazonaws.com/tripdata/202405-citibike-tripdata.zip), [May 2025](https://s3.amazonaws.com/tripdata/202505-citibike-tripdata.zip), and [May 2026](https://s3.amazonaws.com/tripdata/202605-citibike-tripdata.zip). MTA observations are retrieved by the bounded Socrata extractor, which uses only the source columns and aggregations needed for the study. See the [Citi Bike data use policy](https://www.citibikenyc.com/data-sharing-policy), [NYC Open Data policies](https://cityofnewyork.github.io/opendatatsm/publicpolicies.html), and [Open NY terms](https://data.ny.gov/d/77gx-ii52) for source terms.

## Milestone Reports

- [Milestone 2 — Data Analysis and Visualization Sketches](milestones/M2/InfoVizards_Milestone2_Report.pdf)
- [Milestone 3 — First Working Draft](milestones/M3/InfoVizards_Milestone3_Report.pdf)

## AI Assistance

We used OpenAI Codex to implement and debug preprocessing and Observable code, write tests and validation scripts.
