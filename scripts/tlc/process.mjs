import {DuckDBInstance} from '@duckdb/node-api';
import {mkdir,readFile,writeFile,rename,stat,cp} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {sourceTasks,registerSource,literal as q,groupFor} from '../../src/tlc/methods.mjs';
import {acquire,importLocal,save,hashFile} from '../../src/tlc/source.mjs';
import {regression,processSource} from '../../src/tlc/processing.mjs';
import {generate,compareCompletion} from '../../src/tlc/outputs.mjs';
import {validate} from '../../src/tlc/validation.mjs';
const root=fileURLToPath(new URL('../../',import.meta.url)),args=process.argv.slice(2);
const localFile=args.includes('--local-file')?path.resolve(args[args.indexOf('--local-file')+1]):null;
const offline=args.includes('--offline')||Boolean(localFile);
if(!args.includes('--all')&&!args.includes('--regression-only'))throw new Error('Use --all for the approved full window, or --regression-only for cached automatic regression; partial source-month production writes are disabled');
const cache=path.join(root,'.cache/tlc'),production=path.join(root,'data/processed/tlc');
const runId=new Date().toISOString().replaceAll(/[:.]/g,'-'),runDir=path.join(cache,'runs',runId),staging=path.join(runDir,'staging');
await mkdir(staging,{recursive:true});await mkdir(path.join(cache,'tmp'),{recursive:true});
await writeFile(path.join(cache,'latest-run.txt'),runDir+'\n');
const db=await DuckDBInstance.create(':memory:',{extension_directory:path.join(cache,'extensions'),temp_directory:path.join(cache,'tmp'),memory_limit:'1GB',threads:'2',autoinstall_known_extensions:'false',autoload_known_extensions:'false'}),c=await db.connect();
const sources=[],failures=[],registry=new Map();
const resumePath=args.includes('--resume-from')?path.resolve(args[args.indexOf('--resume-from')+1]):null;
let resume;
if(resumePath){
  if(!resumePath.startsWith(path.join(cache,'runs')+path.sep))throw new Error('Resume must use a local TLC staging run');
  resume=JSON.parse(await readFile(path.join(resumePath,'manifest.json'),'utf8'));
  for(const file of ['src/tlc/methods.mjs','src/tlc/processing.mjs'])if(resume.code.find(r=>r.file===file)?.sha256!==await hashFile(path.join(root,file)))throw new Error(`Resume aggregation code changed: ${file}`);
  const priorValidation=JSON.parse(await readFile(path.join(resumePath,'validation.json'),'utf8'));
  const sourceChecks=['Event-month assignment, source boundaries and count reconciliation','Aggregate primary keys','Independent raw-record CRZ controls match staged CRZ cube','DST anomaly reporting preserves flagged events'];
  if(sourceChecks.some(name=>priorValidation.checks.find(r=>r.name===name)?.status!=='passed'))throw new Error('Resume requires previous source numerical checks to pass');
}
if(localFile){
  await stat(localFile);
  if(!resume||resume.sources.length!==23||resume.sources.some(s=>s.service==='fhvhv'&&s.month==='2026-03'))throw new Error('Local completion requires the23source staging run missing only HVFHV March2026');
}

try{
  const regressionResult=resume?.regression?.status==='passed'?{...resume.regression,reused_from:resume.run_id}:await regression(c,cache);await save(path.join(runDir,'regression.json'),regressionResult);console.log(`Cached production-SQL regression PASSED${resume?' (reused)':''}: 631 March-source dropoffs assigned to April`);
  if(args.includes('--regression-only')){console.log(JSON.stringify(regressionResult));}
  else{
    if(!offline){await c.run('LOAD httpfs');await c.run('SET http_timeout=60');await c.run('SET http_retries=0');}
    const zones=JSON.parse(await readFile(path.join(root,'data/geo/v1/zone-groups.json'),'utf8'));
    if(zones.length!==265||zones.filter(z=>z.official_cbd_member).length!==38||new Set(zones.map(z=>z.LocationID)).size!==265)throw new Error('Geography cardinality gate failed');
    await c.run(`CREATE TEMP TABLE zones AS SELECT * FROM read_json_auto(${q(path.join(root,'data/geo/v1/zone-groups.json'))})`);
    for(const {service,month}of sourceTasks()){
      try{
        const source=localFile&&service==='fhvhv'&&month==='2026-03'?await importLocal(c,cache,service,month,localFile):await acquire(c,cache,service,month,offline);registerSource(registry,source);
        const prior=resume?.sources.find(s=>s.service===service&&s.month===month);
        if(prior){
          if(prior.sha256!==source.sha256)throw new Error('Resume source hash changed');
          const oldDir=path.join(resumePath,'sources',`${service}_${month}`),newDir=path.join(staging,'sources',`${service}_${month}`);
          await mkdir(path.dirname(newDir),{recursive:true});
          for(const f of prior.aggregate_files??[])if(await hashFile(path.join(resumePath,f.path))!==f.sha256)throw new Error('Resume aggregate hash mismatch');
          if(!prior.aggregate_files?.length)throw new Error('Resume requires frozen aggregate hashes');
          await cp(oldDir,newDir,{recursive:true,errorOnExist:true,force:false});
          sources.push({...prior,resumed_from:resume.run_id,current_run_source_scans:0});console.log(`Reused validated stage ${service} ${month}`);
        }else sources.push({...await processSource(c,source,staging),current_run_source_scans:1});
      }
      catch(error){const failure={service,month,error:error.stack};failures.push(failure);console.error(`BLOCKED ${service} ${month}: ${error.message}`);await c.run('DROP TABLE IF EXISTS trips');}
      await save(path.join(runDir,'progress.json'),{run_id:runId,staging,processed:sources.map(s=>({service:s.service,month:s.month,source_rows:s.profile.source_rows})),failures});
    }
    for(const source of sources){
      source.aggregate_files=await Promise.all([source.events_file,source.crz_file].map(async file=>({path:file,sha256:await hashFile(path.join(staging,file))})));
      await save(path.join(staging,'sources',`${source.service}_${source.month}`,'manifest.json'),source);
    }
    const products=await generate(c,staging,sources,zones,runId,production);
    const completionComparison=localFile?await compareCompletion(staging,resumePath,products.outputs):null;
    const codeFiles=['scripts/tlc/process.mjs','src/tlc/methods.mjs','src/tlc/source.mjs','src/tlc/processing.mjs','src/tlc/outputs.mjs','src/tlc/validation.mjs','src/tlc/geography.mjs'];
    const manifest={schema_version:'2.0.0',run_id:runId,generated_at:new Date().toISOString(),status:failures.length?'partial':'staged',target_event_months:['03','04','05'],years:[2024,2025,2026],source_months:['02','03','04','05'],timezone:'America/New_York wall-clock interpretation; source TIMESTAMP is naive; no timezone conversion',regression:regressionResult,sources,failures,prior_failures:resume?.failures??[],files:products.files,geography:products.geography,
      geography_sha256:await hashFile(path.join(root,'data/geo/v1/zone-groups.json')),code:await Promise.all(codeFiles.map(async file=>({file,sha256:await hashFile(path.join(root,file))}))),
      previous_comparison:{matched_rows:products.differences.length,changed_rows:products.differences.filter(r=>r.base_difference!==0||r.post_difference!==0||r.old_status!==r.new_status).length},coverage_definition:'Complete approved February-May source files and target event-date coverage; published-source endpoint counts, not a certified vendor census or unique real-world trip census',q5_limitation:'215ordinaryoutside under existing mapping; ten boundary/adjacency subgroup cases remain unresolved',
      network_policy:offline?'disabled':'missing_source_acquisition_only',
      local_completion:localFile?{local_file:localFile,newly_processed:sources.filter(s=>s.current_run_source_scans===1).map(s=>`${s.service}_${s.month}`),reused_sources:sources.filter(s=>s.current_run_source_scans===0).length,network_requests:0}:null,
      resumed_staging:resumePath,completion_comparison:completionComparison};
    if(localFile){
      for(const name of ['march2026-validation.json','q5-review.json'])await cp(path.join(cache,'local-import',name),path.join(staging,name));
    }
    await save(path.join(staging,'manifest.json'),manifest);
    await writeFile(path.join(staging,'README.md'),`# Staged TLC outputs\n\nRun ${runId}. Source files completed: ${sources.length}/24. Production is unchanged unless promotion.json confirms success. Missing files and exact errors: manifest.json. Rows and actual validation: validation.json. Q5 official nonmember totals do not resolve adjacency subgroups.\n`);
    const validation=await validate(c,staging,manifest);console.log(JSON.stringify({validation:validation.status,passed:validation.passed,failed:validation.failed,rows:validation.output_rows,failures}));
    if(failures.length||validation.status!=='passed'){
      manifest.status='not_promoted';await save(path.join(staging,'manifest.json'),manifest);console.error(`Staging preserved without production changes: ${staging}`);process.exitCode=1;
    }else{
      const backup=path.join(cache,'previous',runId);await mkdir(path.dirname(backup),{recursive:true});
      const readme=`# TLC endpoint-event datasets (v2)\n\nTarget event months: March, April, May2024/2025/2026. Source months: February–May for both yellow and HVFHV. Original timestamps and source row multiplicity preserved. Each source read once into DuckDB; no attribute-based trip deduplication. Daily/zone counts retain DST flags; primary hourly profile excludes anomalous wall-hour values and reports sensitivity/exposure.\n\n${Object.entries(validation.output_rows).map(([n,count])=>'- '+n+': '+count+' rows, CSV and JSON.').join('\n')}\n\nAll outputs validated before directory promotion. See manifest.json for source URLs, ETags, retrieval timestamps, schemas, hashes, source rows, exclusions and boundary contributions. validation.json contains actual checks, limitations and DST counts. previous-comparison.json compares the prior implementation. Sources and reusable endpoint/CRZ aggregates remain under sources/; tlc_event_cube.parquet is the combined reusable event cube.\n\nCoverage is the frozen approved published-source population, not a guarantee of vendor completeness or unique actual trips. Official38 taxi-zone CRZ membership approximates the road boundary. Q5 has215ordinaryoutside zones under the existing mapping; ten boundary/adjacency subgroup cases remain unresolved, with exactIDs/candidate evidence in geography-status.json. No north/river/park membership is invented. All changes are descriptive.\n\nFull previous production directory: ${backup}\n\nExplicit reproduction: PATH=/usr/local/bin:$PATH node scripts/tlc/process.mjs --all --offline (no network). Online --all acquires missing files only, with three bounded attempts per source. Tests/install/build never fetch research data.\n`;
      await writeFile(path.join(staging,'README.md'),readme);
      manifest.status='validated';manifest.previous_version=backup;manifest.validation={status:validation.status,passed:validation.passed,failed:validation.failed};await save(path.join(staging,'manifest.json'),manifest);
      let moved=false;
      try{
        if(await stat(production).catch(()=>null)){await rename(production,backup);moved=true;}
        await rename(staging,production);
      }catch(error){if(moved&&!(await stat(production).catch(()=>null)))await rename(backup,production);throw error;}
      await save(path.join(runDir,'promotion.json'),{status:'promoted',production,backup,run_id:runId,validation:manifest.validation,outputs:validation.output_rows});
      console.log(JSON.stringify({status:'promoted',production,backup,rows:validation.output_rows}));
    }
  }
}finally{c.closeSync();db.closeSync();}
