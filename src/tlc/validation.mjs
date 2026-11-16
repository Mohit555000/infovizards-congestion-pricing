import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {csvParse} from 'd3';
import {TABLES,key,datesFor} from './outputs.mjs';
import {SERVICES,YEARS,WINDOWS,literal as q,sourceTasks} from './methods.mjs';
import {query} from './processing.mjs';
import {save,hashFile} from './source.mjs';
const close=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
export async function validate(c,dir,manifest) {
  const results=[],outputs={};
  const check=(name,fn)=>{try{fn();results.push({name,status:'passed'});}catch(e){results.push({name,status:'failed',error:e.message});}};
  for(const name of TABLES){
    const rows=JSON.parse(await readFile(path.join(dir,`${name}.json`),'utf8'));outputs[name]=rows;
    const csv=csvParse(await readFile(path.join(dir,`${name}.csv`),'utf8'));
    check(`${name}: CSV/JSON parity and safe types`,()=>{
      assert.equal(csv.length,rows.length);assert.ok(rows.length>0);
      for(let i=0;i<rows.length;i++)for(const[k,v]of Object.entries(rows[i])){assert.equal(csv[i][k],v===null?'':Array.isArray(v)?JSON.stringify(v):String(v));if(typeof v==='number'){assert.ok(Number.isFinite(v));if(k.endsWith('trips')||k==='trips_entering_crz'||k==='trips_ending_in_crz')assert.ok(Number.isSafeInteger(v));}}
    });
  }
  check('Missing-month coverage',()=>{
    assert.equal(manifest.failures.length,0);assert.equal(manifest.sources.length,24);
    assert.deepEqual(manifest.sources.map(s=>`${s.service}_${s.month}`).sort(),sourceTasks().map(s=>`${s.service}_${s.month}`).sort());
  });
  check('Duplicate-ingestion registry',()=>{
    assert.equal(new Set(manifest.sources.map(s=>s.url)).size,manifest.sources.length);assert.equal(new Set(manifest.sources.map(s=>s.sha256)).size,manifest.sources.length);
    for(const s of manifest.sources){assert.equal(s.source_scan_count,1);assert.equal(s.duplicates_removed,0);assert.equal(s.global_trip_exclusions,0);assert.equal(s.profile.source_rows,s.profile.distinct_ingestion_rows);}
  });
  if(manifest.local_completion){
    const supplied=manifest.sources.find(s=>s.service==='fhvhv'&&s.month==='2026-03');
    check('Local completion processes only one source with no remote access',()=>{
      assert.equal(manifest.network_policy,'disabled');assert.equal(manifest.local_completion.network_requests,0);
      assert.deepEqual(manifest.local_completion.newly_processed,['fhvhv_2026-03']);assert.equal(manifest.local_completion.reused_sources,23);
      for(const s of manifest.sources)assert.equal(s.current_run_source_scans,s===supplied?1:0);
      assert.equal(supplied.acquisition,'user_supplied_local_file');assert.equal(Number(supplied.profile.source_rows),supplied.local_validation.row_count);
      assert.equal(supplied.local_validation.row_count,supplied.local_validation.projected_row_count);assert.equal(supplied.raw_sha256,'0f63d67cb9253b5a818cc4a1b2ddaf3a79955a6bf31c7577c53ff97fa040a262');
      assert.equal(supplied.etag,null);assert.equal(supplied.retrieved_at,null);
      assert.equal(manifest.completion_comparison.status,'passed');
    });
    const review=JSON.parse(await readFile(path.join(dir,'q5-review.json'),'utf8'));
    check('Q5 geometry review and final completion counts',()=>{
      assert.equal(review.status,'passed');assert.equal(review.mapping_sha256,manifest.geography_sha256);
      assert.deepEqual(review.ordinary_outside_ids,manifest.geography.ordinary_outside_ids);
      assert.deepEqual(review.remaining_cases.map(r=>r.LocationID),manifest.geography.adjacency_unresolved_ids);
      for(const r of Object.values(outputs).flat())assert.equal(r.coverage_status,'complete_approved_sources');
    });
  }
  const eventPaths=manifest.sources.map(s=>q(path.resolve(dir,s.events_file))), crzPaths=manifest.sources.map(s=>q(path.resolve(dir,s.crz_file)));
  await c.run(`CREATE OR REPLACE TEMP VIEW validation_events AS SELECT * FROM read_parquet([${eventPaths.join(',')}])`);
  await c.run(`CREATE OR REPLACE TEMP VIEW validation_crz AS SELECT * FROM read_parquet([${crzPaths.join(',')}])`);
  const eventTotals=await query(c,`SELECT service,source_month,metric,substr(date,1,7) event_month,sum(trips) trips FROM validation_events GROUP BY ALL`);
  check('Event-month assignment, source boundaries and count reconciliation',()=>{
    for(const s of manifest.sources){
      assert.equal(s.event_month_distribution.reduce((n,r)=>n+Number(r.trips),0),Number(s.profile.source_rows));
      for(const metric of ['pickup','dropoff']){
        const exclusion=s.event_exclusions.find(e=>e.metric===metric);
        assert.equal(Number(exclusion.input_events),Number(s.profile.source_rows));
        assert.equal(Number(exclusion.input_events),Number(exclusion.retained_events)+Number(exclusion.excluded_null_timestamp)+Number(exclusion.excluded_outside_approved_window));
        const expected=s.event_month_distribution.filter(r=>r[`${metric}_month`]&&['03','04','05'].some(m=>r[`${metric}_month`]===s.month.slice(0,4)+'-'+m)).reduce((n,r)=>n+Number(r.trips),0);
        const actual=eventTotals.filter(r=>r.service===s.service&&r.source_month===s.month&&r.metric===metric).reduce((n,r)=>n+Number(r.trips),0);
        assert.equal(actual,expected);assert.equal(actual,Number(exclusion.retained_events));
        for(const b of s.boundary_contributions.filter(r=>r.metric===metric))assert.equal(Number(b.trips),Number(eventTotals.find(r=>r.service===s.service&&r.source_month===s.month&&r.metric===metric&&r.event_month===b.event_month)?.trips??0));
      }
    }
    const march=eventTotals.find(r=>r.service==='yellow'&&r.source_month==='2024-03'&&r.metric==='dropoff'&&r.event_month==='2024-04');assert.equal(Number(march.trips),631);

  });
  const duplicates=await query(c,`SELECT count(*) n FROM (SELECT service,source_month,metric,date,hour,"LocationID",zone_status,temporal_quality FROM validation_events GROUP BY ALL HAVING count(*)>1)`);
  check('Aggregate primary keys',()=>assert.equal(Number(duplicates[0].n),0));
  const days=await query(c,'SELECT service,metric,date FROM validation_events GROUP BY ALL');
  check('Every approved date has source events for both endpoints',()=>{for(const s of SERVICES)for(const y of YEARS)for(const m of ['pickup','dropoff'])for(const d of datesFor(y,['03','04','05']))assert.ok(days.some(r=>r.service===s&&r.metric===m&&r.date===d),`${s} ${m} ${d}`);});
  const q4=outputs.q4_tlc_zone_change,q5=outputs.q5_tlc_zone_group,q1=outputs.q1_tlc_crz_daily.filter(r=>r.comparison_rule==='calendar_date');
  const unique=(rows,columns)=>assert.equal(new Set(rows.map(r=>key(...columns.map(k=>r[k])))).size,rows.length);
  check('Question primary keys and baseline year matching',()=>{
    unique(outputs.q1_tlc_crz_daily,['service','date','comparison_rule']);unique(outputs.q2_tlc_hour_profile,['window_id','service','year','iso_weekday','hour']);
    unique(q4,['window_id','post_year','service','metric','LocationID','zone_status']);unique(q5,['window_id','post_year','service','metric','zone_group']);unique(outputs.q7_tlc_origin_borough,['window_id','post_year','service','borough']);
    for(const rows of Object.values(outputs))for(const r of rows){assert.equal(r.reference_year,2024);assert.ok(['complete_approved_sources','missing_source'].includes(r.coverage_status));if(r.coverage_status==='missing_source')assert.ok(r.quality_status==='missing_source'||r.matched_months===0);}
    for(const r of outputs.q1_tlc_crz_daily){if(r.period_year===2026)assert.equal(r.comparison_rule,'calendar_date');if(r.reference_date&&r.comparison_rule==='calendar_date')assert.equal(r.reference_date,'2024'+r.date.slice(4));}
  });
  const zoneTotals=await query(c,'SELECT service,substr(date,1,7) AS month,metric,"LocationID",zone_status,sum(trips) trips FROM validation_events GROUP BY ALL');
  const zoneMap=new Map(zoneTotals.map(r=>[key(r.service,r.month,r.metric,r.LocationID,r.zone_status),Number(r.trips)]));
  const amount=(r,y)=>!['yellow','fhvhv'].filter(s=>r.service==='combined'||r.service===s).every(s=>['02','03','04','05'].every(m=>manifest.sources.some(f=>f.service===s&&f.month===`${y}-${m}`)))?null:['yellow','fhvhv'].filter(s=>r.service==='combined'||r.service===s).reduce((n,s)=>n+r.window_id.split(',').reduce((v,m)=>v+(zoneMap.get(key(s,`${y}-${m}`,r.metric,r.LocationID,r.zone_status))??0),0),0);
  check('Q4 independently summed baseline/post totals and low-volume filter',()=>{
    for(const r of q4){assert.equal(r.base_trips,amount(r,2024));assert.equal(r.post_trips,amount(r,r.post_year));if(r.base_trips===null||r.post_trips===null){assert.equal(r.pct_change,null);assert.equal(r.comparison_status,r.base_trips===null?'missing_baseline':'missing_post_period');continue;}assert.equal(r.abs_change,r.post_trips-r.base_trips);close(r.avg_base_trips_per_month,r.base_trips/r.requested_months);assert.equal(r.low_volume,r.base_trips/r.requested_months<1000);if(r.base_trips/r.requested_months>=1000){close(r.pct_change,100*r.abs_change/r.base_trips);assert.equal(r.comparison_status,'valid');}else{assert.equal(r.pct_change,null);assert.equal(r.comparison_status,r.base_trips===0?'zero_baseline':'low_volume');}}
  });
  const groupMap=new Map();for(const r of q4){const k=key(r.window_id,r.post_year,r.service,r.metric,r.zone_group),g=groupMap.get(k)??{base:0,post:0,n:0,low:0};g.base+=r.base_trips;g.post+=r.post_trips;g.n++;if(r.low_volume)g.low++;groupMap.set(k,g);}
  check('Q5 sums all Q4 counts before computing group percentages',()=>{
    for(const r of q5){const g=groupMap.get(key(r.window_id,r.post_year,r.service,r.metric,r.zone_group));if(r.base_trips!==null)assert.equal(r.base_trips,g.base);if(r.post_trips!==null)assert.equal(r.post_trips,g.post);assert.equal(r.n_zones,g.n);assert.equal(r.n_low_volume_zones,g.low);if(r.base_trips!==null&&r.post_trips!==null&&g.base>0)close(r.pct_change,100*(g.post-g.base)/g.base);else assert.equal(r.pct_change,null);}
  });
  const crzDaily=await query(c,"SELECT service,date,sum(trips) ending,sum(trips) FILTER(WHERE origin_class='outside') entering,sum(trips) FILTER(WHERE origin_class='unknown') AS unknown FROM validation_crz GROUP BY ALL");
  const directDaily=new Map(crzDaily.map(r=>[key(r.service,r.date),r]));
  check('Independent raw-record CRZ controls match staged CRZ cube',()=>{
    const controls=new Map();
    for(const s of manifest.sources)for(const r of s.independent_crz_daily_controls){const k=key(s.service,r.date),d=controls.get(k)??{entering:0,ending:0,unknown:0};for(const f of ['entering','ending','unknown'])d[f]+=Number(r[f]);controls.set(k,d);}
    for(const r of crzDaily){const d=controls.get(key(r.service,r.date));assert.ok(d);for(const f of ['entering','ending','unknown'])assert.equal(Number(r[f]??0),d[f]);}
    assert.equal(controls.size,crzDaily.length);
  });

  const dailyAmount=(s,d,f)=>['yellow','fhvhv'].filter(v=>s==='combined'||s===v).reduce((n,v)=>n+Number(directDaily.get(key(v,d))?.[f]??0),0);
  check('Q1 daily counts, known/unknown origins, references and seven-calendar-day means',()=>{
    for(const r of outputs.q1_tlc_crz_daily){if(r.trips_entering_crz===null){assert.equal(r.coverage_status,'missing_source');assert.equal(r.trips_ending_in_crz,null);assert.equal(r.pct_change,null);continue;}assert.equal(r.trips_entering_crz,dailyAmount(r.service,r.date,'entering'));assert.equal(r.trips_ending_in_crz,dailyAmount(r.service,r.date,'ending'));assert.equal(r.unknown_origin_trips,dailyAmount(r.service,r.date,'unknown'));assert.ok(r.trips_entering_crz+r.unknown_origin_trips<=r.trips_ending_in_crz);
      if(r.reference_entering!==null){const base=dailyAmount(r.service,r.reference_date,'entering');assert.equal(r.reference_entering,base);if(base>0)close(r.pct_change,100*(r.trips_entering_crz-base)/base);else assert.equal(r.pct_change,null);}
      if(r.entering_ma7!==null){const ds=Array.from({length:7},(_,i)=>new Date(Date.parse(r.date+'T00:00:00Z')-i*86400000).toISOString().slice(0,10));assert.ok(ds.every(d=>q1.some(t=>t.service===r.service&&t.date===d)));close(r.entering_ma7,ds.reduce((n,d)=>n+dailyAmount(r.service,d,'entering'),0)/7);}
    }
  });
  const dailyWindow=(s,y,months,field)=>q1.filter(r=>r.service===s&&r.period_year===y&&months.includes(r.date.slice(5,7))).reduce((n,r)=>n+r[field],0);
  check('Q7 borough totals match Q1 entering and unknown-origin counts',()=>{
    for(const service of [...SERVICES,'combined'])for(const year of [2025,2026])for(const months of WINDOWS){const rows=outputs.q7_tlc_origin_borough.filter(r=>r.service===service&&r.post_year===year&&r.window_id===months.join(','));assert.equal(rows.reduce((n,r)=>n+r.base_trips,0),dailyWindow(service,2024,months,'trips_entering_crz'));assert.equal(rows.reduce((n,r)=>n+r.post_trips,0),dailyWindow(service,year,months,'trips_entering_crz'));assert.equal(rows.reduce((n,r)=>n+r.unknown_origin_trips,0),dailyWindow(service,year,months,'unknown_origin_trips'));for(const r of rows){if(r.base_trips===null||r.post_trips===null){assert.equal(r.pct_change,null);continue;}assert.equal(r.abs_change,r.post_trips-r.base_trips);if(r.base_trips>0)close(r.pct_change,100*r.abs_change/r.base_trips);else assert.equal(r.pct_change,null);}}
  });
  check('Q2 hourly sensitivity reconciles with Q1; exposures and normalization',()=>{
    const groups=new Map();for(const r of outputs.q2_tlc_hour_profile){if(r.trips_entering===null){assert.equal(r.coverage_status,'missing_source');assert.equal(r.avg_trips_entering_per_day,null);assert.equal(r.n_days,0);continue;}const k=key(r.service,r.year,r.window_id),g=groups.get(k)??{trips:0,profile:[]};g.trips+=r.trips_entering_including_dst;g.profile.push(r);groups.set(k,g);assert.equal(r.trips_entering+r.dst_anomaly_trips_excluded,r.trips_entering_including_dst);
      const days=datesFor(r.year,r.window_id.split(',')).filter(d=>(new Date(d+'T00:00:00Z').getUTCDay()||7)===r.iso_weekday);const gap=r.hour===2&&r.iso_weekday===7&&r.window_id.split(',').includes('03')?1:0;assert.equal(r.n_days,days.length-gap);if(r.n_days>0)close(r.avg_trips_entering_per_day,r.trips_entering/r.n_days);if(r.normalized_peak_pct!==null){assert.ok(r.normalized_peak_pct>=0&&r.normalized_peak_pct<=100+1e-10);close(r.normalized_peak_pct,100*r.avg_trips_entering_per_day/r.profile_peak);}}
    for(const [k,g] of groups){const[s,y,w]=JSON.parse(k);assert.equal(g.trips,dailyWindow(s,y,w.split(','),'trips_entering_crz'));for(let d=1;d<=7;d++){const profile=g.profile.filter(r=>r.iso_weekday===d);assert.equal(profile.length,24);if(profile[0].profile_peak>0)close(Math.max(...profile.map(r=>r.normalized_peak_pct??0)),100);}}
  });
  const eventDST=await query(c,"SELECT service,source_month,metric,date,hour,temporal_quality,sum(trips) trips FROM validation_events WHERE temporal_quality!='ordinary' GROUP BY ALL");
  check('DST anomaly reporting preserves flagged events',()=>{
    for(const r of eventDST){const s=manifest.sources.find(s=>s.service===r.service&&s.month===r.source_month),original=s.dst_anomalies.find(a=>a.metric===r.metric&&a.date===r.date&&a.hour===r.hour&&a.temporal_quality===r.temporal_quality);assert.ok(original);assert.equal(Number(r.trips),Number(original.trips));assert.equal(r.hour,2);assert.equal(r.temporal_quality,'spring_gap');}
    assert.equal(Number(eventDST.filter(r=>r.service==='fhvhv'&&r.source_month==='2024-03'&&r.metric==='pickup').reduce((n,r)=>n+Number(r.trips),0)),79);
  });
  check('Geography remains explicit and exhaustive',()=>{
    assert.equal(manifest.geography.verified_inside_ids.length,38);assert.equal(manifest.geography.ordinary_outside_ids.length,215);
    assert.deepEqual(manifest.geography.adjacency_unresolved_ids,[33,34,43,66,140,141,142,143,145,237]);
    assert.equal(new Set([...manifest.geography.verified_inside_ids,...manifest.geography.ordinary_outside_ids,...manifest.geography.adjacency_unresolved_ids]).size,263);
    for(const r of q5.filter(r=>r.zone_group==='unresolved'))assert.equal(r.unresolved_zone_ids.length,10);
    for(const r of q4.filter(r=>r.LocationID!==null&&r.LocationID<=263)){const expected=manifest.geography.verified_inside_ids.includes(r.LocationID)?'inside':manifest.geography.ordinary_outside_ids.includes(r.LocationID)?'outside':'unresolved';assert.equal(r.zone_group,expected);}
  });
  for(const source of manifest.sources)for(const file of source.aggregate_files??[]){const actual=await hashFile(path.join(dir,file.path));check(`${file.path}: SHA-256`,()=>assert.equal(actual,file.sha256));}
  for(const file of manifest.files){const actual=await hashFile(path.join(dir,file.path));check(`${file.path}: SHA-256`,()=>assert.equal(actual,file.sha256));}
  const differences=JSON.parse(await readFile(path.join(dir,'previous-comparison.json'),'utf8'));
  const boundaryZone=await query(c,`SELECT service,substr(date,1,7) AS month,metric,"LocationID",zone_status,sum(trips) trips FROM validation_events WHERE source_month!=substr(date,1,7) GROUP BY ALL`);
  const boundaryMap=new Map(boundaryZone.map(r=>[key(r.service,r.month,r.metric,r.LocationID,r.zone_status),Number(r.trips)]));
  check('Previous implementation comparison: changes equal retained boundary events',()=>{
    for(const r of differences)for(const [y,oldValue,newValue,diff]of [[2024,r.old_base,r.new_base,r.base_difference],[r.post_year,r.old_post,r.new_post,r.post_difference]]){
      if(oldValue===null||newValue===null)continue;
      const expected=['yellow','fhvhv'].filter(s=>r.service==='combined'||r.service===s).reduce((n,s)=>n+r.window_id.split(',').reduce((v,m)=>v+(boundaryMap.get(key(s,`${y}-${m}`,r.metric,r.LocationID,r.LocationID>263?'nonspatial':'spatial'))??0),0),0);
      assert.equal(diff,expected,JSON.stringify(r));
    }
  });
  const report={status:results.every(r=>r.status==='passed')?'passed':'failed' ,checks:results,passed:results.filter(r=>r.status==='passed').length,failed:results.filter(r=>r.status==='failed').length,source_files:manifest.sources.length,source_rows:manifest.sources.reduce((n,s)=>n+Number(s.profile.source_rows),0),output_rows:Object.fromEntries(Object.entries(outputs).map(([n,r])=>[n,r.length])),dst_anomalies:eventDST,limitations:['Bounded observed-source completeness; vendor completeness and timestamp errors outside approved files cannot be certified','Official CBD taxi zones approximate road CRZ; ten boundary/adjacency subgroup cases remain unresolved;215ordinaryoutside under the existing mapping','Naive timestamp semantics; DST values preserved; primary hourly profile excludes flagged anomalies','Threshold1000/month is an approved display rule, not a statistical reliability guarantee']};
  await save(path.join(dir,'validation.json'),report);return report;
}
