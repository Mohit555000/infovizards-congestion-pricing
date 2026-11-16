import {DuckDBInstance} from '@duckdb/node-api';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {validate} from '../../src/tlc/validation.mjs';
const dir=path.resolve(process.argv[2]??'data/processed/tlc');
const manifest=JSON.parse(await readFile(path.join(dir,'manifest.json'),'utf8'));
const db=await DuckDBInstance.create(':memory:',{memory_limit:'1GB',threads:'2',temp_directory:path.resolve('.cache/tlc/tmp')}),c=await db.connect();
try{const report=await validate(c,dir,manifest);console.log(JSON.stringify(report,null,2));if(report.status!=='passed')process.exitCode=1;}finally{c.closeSync();db.closeSync();}
