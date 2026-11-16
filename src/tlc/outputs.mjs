import {readFile,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {csvFormat} from 'd3';
import {SERVICES,YEARS,WINDOWS,literal as q,comparison,dailyReference} from './methods.mjs';
import {query} from './processing.mjs';
import {hashFile,save} from './source.mjs';
import {groupFor,geographyStatus} from './geography.mjs';
export const TABLES=['q1_tlc_crz_daily','q2_tlc_hour_profile','q4_tlc_zone_change','q5_tlc_zone_group','q7_tlc_origin_borough'];
export const key = (...parts)=>JSON.stringify(parts);
const sum = values=>values.reduce((a,b)=>a+Number(b),0);
const calendar = year=>Array.from({length:92},(_,i)=>new Date(Date.UTC(year,2,1+i)).toISOString().slice(0,10));
export const datesFor = (year,months)=>calendar(year).filter(d=>months.includes(d.slice(5,7)));
export function ratio(base,post,complete) {
  const status=base===null?'missing_baseline':post===null?'missing_post_period':!complete?'partial_coverage':base===0?'zero_baseline':'valid';
  return {base_trips:base,post_trips:post,abs_change:complete&&base!==null&&post!==null?post-base:null,pct_change:status==='valid'?100*(post-base)/base:null,comparison_status:status,baseline_status:base===null?'missing_baseline':base===0?'zero_baseline':'observed_positive'};
}
export async function generate(c,staging,manifests,zones,runId,previous) {
  const paths=manifests.map(m=>path.resolve(staging,m.events_file)),crzPaths=manifests.map(m=>path.resolve(staging,m.crz_file));
  if(!paths.length)throw new Error('No successfully processed sources');
  await c.run(`CREATE OR REPLACE TEMP TABLE all_events AS SELECT * FROM read_parquet([${paths.map(q).join(',')}])`);
  await c.run(`CREATE OR REPLACE TEMP TABLE all_crz AS SELECT * FROM read_parquet([${crzPaths.map(q).join(',')}])`);
  await c.run(`COPY (SELECT service,metric,date,hour,"LocationID",zone_status,temporal_quality,sum(trips)::BIGINT trips FROM all_events GROUP BY ALL ORDER BY ALL) TO ${q(path.join(staging,'tlc_event_cube.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)`);
  const zoneTotals=await query(c,`SELECT service,substr(date,1,7) AS month,metric,"LocationID",zone_status,sum(trips) trips FROM all_events GROUP BY ALL`);
  const crzHours=await query(c,`SELECT service,date,hour,borough,origin_class,temporal_quality,sum(trips) trips FROM all_crz GROUP BY ALL`);
  const counts=new Map(zoneTotals.map(r=>[key(r.service,r.month,r.metric,r.LocationID,r.zone_status),Number(r.trips)]));
  const daily=new Map(),hours=new Map(),boroughCounts=new Map();
  for(const r of crzHours){
    const dailyKey=key(r.service,r.date),d=daily.get(dailyKey)??{entering:0,ending:0,unknown:0};
    d.ending+=Number(r.trips);if(r.origin_class==='outside')d.entering+=Number(r.trips);if(r.origin_class==='unknown')d.unknown+=Number(r.trips);daily.set(dailyKey,d);
    if(r.origin_class==='outside'){
      const hkey=key(r.service,r.date,r.hour),h=hours.get(hkey)??{ordinary:0,anomalous:0};
      h[r.temporal_quality==='ordinary'?'ordinary':'anomalous']+=Number(r.trips);hours.set(hkey,h);
    }
    const bkey=key(r.service,r.date.slice(0,7),r.borough),b=boroughCounts.get(bkey)??{entering:0,unknown:0};
    if(r.origin_class==='outside')b.entering+=Number(r.trips);if(r.origin_class==='unknown')b.unknown+=Number(r.trips);boroughCounts.set(bkey,b);
  }
  const components=s=>s==='combined'?SERVICES:[s];
  const complete=(service,year)=>components(service).every(s=>['02','03','04','05'].every(m=>manifests.some(f=>f.service===s&&f.month===`${year}-${m}`)));
  const common=(service,year)=>({schema_version:'2.0.0',run_id:runId,quality_status:complete(service,year)?'observed':'missing_source',missing_reason:complete(service,year)?null:'missing_approved_source_file',geography_version:'official-cbd-v1-outside-review-2026-11-15',quality_flags:['provider_completeness_not_certified','naive_wall_timestamps','zone_crz_approximation'],population:'February-May published source rows; endpoint in March-May; no trip-attribute deduplication'});
  const windowMeta=(service,year,months)=>({
    ...common(service,year),window_id:months.join(','),post_year:year,reference_year:2024,comparison_rule:'same_calendar_months_reference_2024',
    post_start:`${year}-${months[0]}-01`,post_end:datesFor(year,months).at(-1),reference_start:`2024-${months[0]}-01`,reference_end:datesFor(2024,months).at(-1),
    requested_months:months.length,matched_months:complete(service,2024)&&complete(service,year)?months.length:0,n_months:months.length,
    base_days:datesFor(2024,months).length,post_days:datesFor(year,months).length,
    coverage_status:complete(service,2024)&&complete(service,year)?'complete_approved_sources':'missing_source',period_coverage_status:complete(service,2024)&&complete(service,year)?'complete_approved_sources':'partial_or_unverified',
    quality_flags:['provider_completeness_not_certified','naive_wall_timestamps','zone_crz_approximation']});
  const atDay=(s,date)=>{const a=components(s).map(v=>daily.get(key(v,date))??{entering:0,ending:0,unknown:0});return {entering:sum(a.map(d=>d.entering)),ending:sum(a.map(d=>d.ending)),unknown:sum(a.map(d=>d.unknown))};};
  const outputs=Object.fromEntries(TABLES.map(n=>[n,[]]));
  for(const service of [...SERVICES,'combined'])for(const year of YEARS)for(const date of calendar(year)){
    const observed=complete(service,year),d=atDay(service,date);
    const trailing=Array.from({length:7},(_,i)=>new Date(Date.parse(date+'T00:00:00Z')-i*86400000).toISOString().slice(0,10));
    const eligible=trailing.filter(t=>t>=`${year}-03-01`&&t<=`${year}-05-31`&&observed);
    for(const rule of year===2025?['calendar_date','weekday_364']:['calendar_date']){
      const reference=year===2024?null:dailyReference(date,rule),supported=reference&&reference>='2024-03-01'&&reference<='2024-05-31'&&complete(service,2024);
      const base=supported?atDay(service,reference).entering:null;
      outputs.q1_tlc_crz_daily.push({...common(service,year),service,date,period_year:year,comparison_rule:rule,day_of_week:new Date(date+'T00:00:00Z').getUTCDay()||7,
        trips_entering_crz:observed?d.entering:null,trips_ending_in_crz:observed?d.ending:null,unknown_origin_trips:observed?d.unknown:null,
        entering_ma7:eligible.length===7?sum(trailing.map(t=>atDay(service,t).entering))/7:null,ma7_complete_days:eligible.length,
        reference_date:reference,reference_year:2024,reference_entering:base,pct_change:observed&&base>0?100*(d.entering-base)/base:null,
        reference_status:year===2024?'baseline_year':supported?'observed':'unavailable_reference',coverage_status:observed?'complete_approved_sources':'missing_source'});
    }
  }
  for(const service of [...SERVICES,'combined'])for(const months of WINDOWS){
    const zoneAmount=(year,metric,z)=>complete(service,year)?sum(components(service).flatMap(s=>months.map(m=>counts.get(key(s,`${year}-${m}`,metric,z.LocationID,z.zone_status))??0))):null;
    const zoneList=[...zones.map(z=>({...z,zone_status:z.LocationID>263?'nonspatial':'spatial',zone_group:groupFor(z)})),
      {LocationID:null,zone:'Missing zone ID',borough:null,zone_status:'missing',zone_group:'missing'},
      {LocationID:null,zone:'Invalid zone ID',borough:null,zone_status:'invalid',zone_group:'invalid'}];
    for(const year of [2025,2026])for(const metric of ['pickup','dropoff']){
      const grouped=new Map();
      for(const z of zoneList){
        const base=zoneAmount(2024,metric,z),post=zoneAmount(year,metric,z),both=base!==null&&post!==null;
        const comp=comparison(base,post,months.length,{basePresent:base!==null,postPresent:post!==null,complete:both});
        const row={...windowMeta(service,year,months),...comp,period_coverage_status:both?'complete_approved_sources':'partial_or_unverified',service,metric,
          LocationID:z.LocationID,zone_status:z.zone_status,zone:z.zone,borough:z.borough,zone_group:z.zone_group,threshold:1000,
          adjacency_status:z.zone_group==='unresolved'?'unresolved':z.zone_group==='outside'?'ordinary_outside_existing_mapping':'not_applicable'};
        outputs.q4_tlc_zone_change.push(row);
        const g=grouped.get(z.zone_group)??{base:0,post:0,n:0,observed:0,low:0,ids:[]};
        if(base!==null)g.base+=base;if(post!==null)g.post+=post;g.n++;if((base??0)+(post??0)>0)g.observed++;if(comp.low_volume)g.low++;if(z.LocationID!==null)g.ids.push(z.LocationID);grouped.set(z.zone_group,g);
      }
      for(const [group,g] of grouped)outputs.q5_tlc_zone_group.push({...windowMeta(service,year,months),...ratio(complete(service,2024)?g.base:null,complete(service,year)?g.post:null,complete(service,2024)&&complete(service,year)),service,metric,zone_group:group,n_zones:g.n,n_observed_zones:g.observed,n_low_volume_zones:g.low,
        classification_status:group==='unresolved'?'unresolved_boundary_or_adjacency':group==='outside'?'ordinary_outside_existing_mapping':'verified_or_explicit_unknown',unresolved_zone_ids:group==='unresolved'?g.ids:[]});
    }
    for(const year of YEARS)for(let dow=1;dow<=7;dow++){
      const days=datesFor(year,months).filter(d=>(new Date(d+'T00:00:00Z').getUTCDay()||7)===dow),profile=[];
      for(let hour=0;hour<24;hour++){
        const validDays=days.filter(d=>!(hour===2&&dow===7&&d.slice(5,7)==='03'&&Number(d.slice(8))>=8&&Number(d.slice(8))<=14));
        const h=components(service).flatMap(s=>days.map(d=>hours.get(key(s,d,hour))??{ordinary:0,anomalous:0}));
        const trips=sum(h.map(r=>r.ordinary)),anomalies=sum(h.map(r=>r.anomalous)),present=complete(service,year);
        profile.push({...common(service,year),window_id:months.join(','),service,year,iso_weekday:dow,hour,day_type:dow<6?'weekday':'weekend',trips_entering:present?trips:null,
          dst_anomaly_trips_excluded:present?anomalies:null,trips_entering_including_dst:present?trips+anomalies:null,n_days:present?validDays.length:0,expected_days:validDays.length,calendar_weekday_days:days.length,
          avg_trips_entering_per_day:present&&validDays.length?trips/validDays.length:null,reference_year:2024,comparison_rule:'same_calendar_months_reference_2024',coverage_status:present?'complete_approved_sources':'missing_source'});
      }
      const peak=complete(service,year)?Math.max(...profile.map(r=>r.avg_trips_entering_per_day??0)):null;
      for(const r of profile)outputs.q2_tlc_hour_profile.push({...r,profile_peak:peak,normalized_peak_pct:peak>0&&r.avg_trips_entering_per_day!==null?100*r.avg_trips_entering_per_day/peak:null});
    }
    const knownBoroughs=[...new Set(zones.filter(z=>z.LocationID<=263).map(z=>z.borough))].sort();
    const boroughs=[...knownBoroughs,'missing','invalid','nonspatial_264','nonspatial_265'];
    const boroughAmount=(year,borough,field)=>complete(service,year)?sum(components(service).flatMap(s=>months.map(m=>(boroughCounts.get(key(s,`${year}-${m}`,borough))??{entering:0,unknown:0})[field]))):null;
    for(const year of [2025,2026])for(const borough of boroughs)outputs.q7_tlc_origin_borough.push({...windowMeta(service,year,months),...ratio(boroughAmount(2024,borough,'entering'),boroughAmount(year,borough,'entering'),complete(service,2024)&&complete(service,year)),service,borough,
      base_unknown_origin_trips:boroughAmount(2024,borough,'unknown'),unknown_origin_trips:boroughAmount(year,borough,'unknown'),origin_status:knownBoroughs.includes(borough)?'known_source_borough_category':'unclassified_origin_excluded_from_entering'});
  }
  const ref=new Map(outputs.q2_tlc_hour_profile.filter(r=>r.year===2024).map(r=>[key(r.window_id,r.service,r.iso_weekday,r.hour),r.avg_trips_entering_per_day]));
  for(const r of outputs.q2_tlc_hour_profile){r.reference_avg=r.year===2024?null:ref.get(key(r.window_id,r.service,r.iso_weekday,r.hour));r.difference_trips_per_day=r.avg_trips_entering_per_day!==null&&r.reference_avg!==null?r.avg_trips_entering_per_day-r.reference_avg:null;}
  const files=[];
  for(const [name,rows]of Object.entries(outputs)){
    // Deterministic construction order and numeric values; no display rounding.
    for(const ext of ['json','csv']){
      const file=path.join(staging,`${name}.${ext}`);
      if(ext==='json')await save(file,rows);else await writeFile(file,csvFormat(rows.map(r=>Object.fromEntries(Object.entries(r).map(([k,v])=>[k,Array.isArray(v)?JSON.stringify(v):v]))))+'\n');
      files.push({name,format:ext,path:path.basename(file),rows:rows.length,sha256:await hashFile(file)});
    }
  }
  const old=await readFile(path.join(previous,'q4_tlc_zone_change.json'),'utf8').then(JSON.parse).catch(()=>[]);
  const oldKey=r=>key(r.window_id,r.post_year,r.service,r.metric,r.LocationID);
  const oldMap=new Map(old.map(r=>[oldKey(r),r]));
  const differences=outputs.q4_tlc_zone_change.filter(r=>r.LocationID!==null&&oldMap.has(oldKey(r))).map(r=>{
    const before=oldMap.get(oldKey(r));return {window_id:r.window_id,post_year:r.post_year,service:r.service,metric:r.metric,LocationID:r.LocationID,old_base:before.base_trips,new_base:r.base_trips,base_difference:before.base_trips!==null&&r.base_trips!==null?r.base_trips-before.base_trips:null,old_post:before.post_trips,new_post:r.post_trips,post_difference:before.post_trips!==null&&r.post_trips!==null?r.post_trips-before.post_trips:null,old_status:before.comparison_status,new_status:r.comparison_status};
  });
  await save(path.join(staging,'previous-comparison.json'),differences);
  const geography=geographyStatus(zones);
  await save(path.join(staging,'geography-status.json'),geography);
  return {outputs,files,zoneTotals,crzHours,geography,differences};
}

export async function compareCompletion(staging, previousStage, outputs) {
  const results=[];
  const fields={q1_tlc_crz_daily:['service','date','comparison_rule'],q2_tlc_hour_profile:['window_id','service','year','iso_weekday','hour'],q4_tlc_zone_change:['window_id','post_year','service','metric','LocationID','zone_status'],q7_tlc_origin_borough:['window_id','post_year','service','borough']};
  for(const name of TABLES){
    const before=JSON.parse(await readFile(path.join(previousStage,`${name}.json`),'utf8')),after=outputs[name];
    let unchanged=0,filled=0;
    if(name==='q5_tlc_zone_group'){
      const coarsen=rows=>{const totals=new Map();for(const r of rows){const group=['outside','unresolved','outside_crz_adjacency_unresolved'].includes(r.zone_group)?'all_official_nonmembers':r.zone_group;
        const k=key(r.window_id,r.post_year,r.service,r.metric,group),v=totals.get(k)??{base:0,post:0};
        v.base=r.base_trips===null?null:v.base===null?null:v.base+r.base_trips;v.post=r.post_trips===null?null:v.post===null?null:v.post+r.post_trips;totals.set(k,v);}return totals;};
      const old=coarsen(before),next=coarsen(after);
      for(const [k,v]of old){const n=next.get(k);if(!n)throw new Error('Q5 comparison lost group');for(const f of ['base','post']){if(v[f]!==null){if(v[f]!==n[f])throw new Error(`Q5 complete prior group count changed: ${k}`);unchanged++;}else if(n[f]!==null)filled++;}}
    }else{
      const pk=r=>key(...fields[name].map(k=>r[k])),next=new Map(after.map(r=>[pk(r),r]));
      const measures=name==='q1_tlc_crz_daily'?['trips_entering_crz','trips_ending_in_crz','unknown_origin_trips']:name==='q2_tlc_hour_profile'?['trips_entering','trips_entering_including_dst','dst_anomaly_trips_excluded']:['base_trips','post_trips'];
      for(const r of before){const n=next.get(pk(r));if(!n)throw new Error(`${name} comparison lost key`);for(const f of measures){if(r[f]!==null){if(r[f]!==n[f])throw new Error(`${name} prior observed count changed: ${pk(r)} ${f}`);unchanged++;}else if(n[f]!==null)filled++;}}
    }
    results.push({name,previous_rows:before.length,new_rows:after.length,unchanged_observed_count_fields:unchanged,missing_count_fields_now_observed:filled,status:'passed'});
  }
  const report={status:'passed',previous_staging:previousStage,tables:results,interpretation:'Prior observed counts and2024baselines unchanged; previously missing2026HVFHV/combined fields become observed. Q5ordinaryoutside+unresolved equals prior225nonmember union; missing values were not treated as zero.'};
  await save(path.join(staging,'completion-comparison.json'),report);return report;
}
