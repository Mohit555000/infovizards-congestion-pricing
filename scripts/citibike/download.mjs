import{mkdir,readFile,writeFile,rename,stat}from'node:fs/promises';import{createReadStream,createWriteStream}from'node:fs';import{Readable}from'node:stream';import{pipeline}from'node:stream/promises';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';
const root=process.cwd()+'/.cache/citibike';await mkdir(root,{recursive:true});
for(const year of[2024,2025,2026]){
 const file=`${root}/${year}05.zip`,url=`https://s3.amazonaws.com/tripdata/${year}05-citibike-tripdata.zip`;let exists=false;try{await stat(file);exists=true;}catch{}
 if(!exists){let ok=false;for(let attempt=0;attempt<3;attempt++){try{const r=await fetch(url,{signal:AbortSignal.timeout(240000)});if(!r.ok)throw new Error(`HTTP${r.status}`);let bytes=0;const counted=async function*(stream){for await(const chunk of stream){bytes+=chunk.length;if(bytes>1_000_000_000)throw new Error('Archive exceeds1GB cap');yield chunk;}};await pipeline(counted(Readable.fromWeb(r.body)),createWriteStream(file+'.part'));await rename(file+'.part',file);ok=true;break;}catch(e){if(attempt===2)throw e;await new Promise(r=>setTimeout(r,1000*(attempt+1)));}}if(!ok)throw new Error('Download failed');}
 const hash=createHash('sha256');for await(const chunk of createReadStream(file))hash.update(chunk);const sha256=hash.digest('hex');
 let old;try{old=JSON.parse(await readFile(`${root}/${year}-manifest.json`));}catch{}
 if(old&&old.sha256!==sha256)throw new Error('Cached archive changed; review required');
 const members=execFileSync('/usr/bin/unzip',['-Z1',file],{encoding:'utf8'}).trim().split('\n').filter(n=>n.endsWith('.csv')&&!n.includes('__MACOSX'));
 if(!members.length||members.some(n=>n.startsWith('/')||n.split('/').includes('..')))throw new Error('Unsafe/empty ZIP inventory');
 execFileSync('/usr/bin/unzip',['-tq',file],{stdio:'pipe'});await mkdir(`${root}/${year}`,{recursive:true});execFileSync('/usr/bin/unzip',['-oq',file,'-d',`${root}/${year}`]);
 await writeFile(`${root}/${year}-manifest.json`,JSON.stringify({url,bytes:(await stat(file)).size,sha256,csv_members:members},null,2));console.log(year,members.length,'CSV members verified');
}
