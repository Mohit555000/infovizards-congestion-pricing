import {DuckDBInstance}from'@duckdb/node-api';
import{readFile,writeFile,mkdir}from'node:fs/promises';
import{csvParse,csvFormat,geoArea,geoContains,geoMercator,geoPath}from'd3';
const root=process.cwd(),out=root+'/data/geo/v1';await mkdir(out,{recursive:true});
const db=await DuckDBInstance.create(':memory:',{extension_directory:root+'/.cache/tlc/extensions'});const c=await db.connect();
const rows=async s=>(await c.runAndReadAll(s)).getRowObjectsJson();
try{
 await c.run('LOAD spatial');
 await c.run("CREATE TABLE zones AS SELECT LocationID,zone,borough,geom AS projected,ST_Transform(geom,'EPSG:2263','EPSG:4326',always_xy:=true) AS geom FROM ST_Read('.cache/geo/official/taxi_zones/taxi_zones.shp')");
 const quality=await rows('SELECT count(*) AS n,count(distinct LocationID) AS ids,count(*) FILTER(WHERE NOT ST_IsValid(projected)) AS invalid,count(*) FILTER(WHERE ST_IsEmpty(projected)) AS empty FROM zones');
 if(Number(quality[0].n)!==263||Number(quality[0].ids)!==263||Number(quality[0].invalid)||Number(quality[0].empty))throw new Error('Geometry integrity failed; no silent repair');
 const cbd=JSON.parse(await readFile('.cache/geo/cbd-zones.json')),ids=cbd.map(r=>Number(r.taxi_zone)).sort((a,b)=>a-b);
 if(new Set(ids).size!==ids.length)throw new Error('CBD duplicate IDs');
 const fence=JSON.parse(await readFile('.cache/geo/geofence.json'));await writeFile(out+'/official-crz-geofence.geojson',JSON.stringify(fence)+'\n');
 await c.run("CREATE TABLE fence AS SELECT ST_Union_Agg(ST_Transform(geom,'EPSG:4326','EPSG:2263',always_xy:=true)) AS geom FROM ST_Read('data/geo/v1/official-crz-geofence.geojson')");
 const validity=await rows('SELECT ST_IsValid(geom) AS valid FROM fence');if(!validity[0].valid)throw new Error('Official geofence invalid; review required');
 await c.run(`CREATE TABLE inside AS SELECT ST_Union_Agg(projected) AS geom FROM zones WHERE LocationID IN(${ids.join(',')})`);
 const mapped=await rows(`SELECT z.LocationID,z.zone,z.borough,ST_Area(z.projected) AS area_ft2,ST_Area(ST_Intersection(z.projected,f.geom))/ST_Area(z.projected) AS crz_area_fraction,ST_Distance(z.projected,i.geom)*0.304800609601219 AS distance_to_cbd_m,ST_Length(ST_Intersection(ST_Boundary(z.projected),ST_Boundary(i.geom))) AS shared_boundary_ft,ST_AsGeoJSON(z.geom) AS geometry FROM zones z,fence f,inside i ORDER BY z.LocationID`);
 const mapping=mapped.map(r=>{const inside=ids.includes(r.LocationID),north=!inside&&r.borough==='Manhattan'&&r.shared_boundary_ft>1,river=['Brooklyn','Queens'].includes(r.borough)&&r.distance_to_cbd_m<=500;
 const partial=r.crz_area_fraction>0.000001&&r.crz_area_fraction<0.999999;
 return{LocationID:r.LocationID,zone:r.zone,borough:r.borough,official_cbd_member:inside,zone_group:inside?'inside':r.LocationID===43?'ambiguous_central_park':north?'adjacent_north':river?'adjacent_across_river':'outside',rule_version:'1.0.0-review',classification_status:inside?'official_taxi_zone_classification':r.LocationID===43?'ambiguous':north||river?'proposed_operational_adjacency':'outside_operational_groups',crz_area_fraction:r.crz_area_fraction,polygon_relation:r.crz_area_fraction>=0.999999?'fully_inside_geofence':r.crz_area_fraction<=0.000001?'outside_geofence':'partial_intersection',shared_boundary_ft:r.shared_boundary_ft,distance_to_cbd_m:r.distance_to_cbd_m,point_vs_zone_warning:partial};});
 const lookup=csvParse(await readFile('.cache/geo/lookup.csv','utf8'));for(const r of lookup){const match=mapping.find(m=>m.LocationID===Number(r.LocationID));if(match&&(match.borough!==r.Borough||match.zone!==r.Zone))throw new Error('Zone name/borough mismatch');}
 for(const r of lookup.filter(r=>!mapping.some(m=>m.LocationID===Number(r.LocationID))))mapping.push({LocationID:Number(r.LocationID),zone:r.Zone,borough:r.Borough,official_cbd_member:null,zone_group:'nonspatial',classification_status:'no_polygon',rule_version:'1.0.0-review'});
 const geo={type:'FeatureCollection',features:mapped.map(r=>({type:'Feature',id:r.LocationID,properties:mapping.find(m=>m.LocationID===r.LocationID),geometry:JSON.parse(r.geometry)}))};
 // Canonical RFC7946 planar winding: exterior CCW, holes CW. D3 variant below is separate.
 for(const f of geo.features){const polys=f.geometry.type==='Polygon'?[f.geometry.coordinates]:f.geometry.coordinates;for(const poly of polys)for(let i=0;i<poly.length;i++){const ring=poly[i];const area=ring.slice(1).reduce((sum,p,j)=>sum+ring[j][0]*p[1]-p[0]*ring[j][1],0);if((i===0&&area<0)||(i>0&&area>0))ring.reverse();}}
 await writeFile(out+'/taxi-zones.geojson',JSON.stringify(geo)+'\n');
 // D3 uses spherical winding; provide a separately labeled variant, never silently change canonical GeoJSON.
 const d3geo=structuredClone(geo);for(const f of d3geo.features){if(geoArea(f)>2*Math.PI){const polys=f.geometry.type==='Polygon'?[f.geometry.coordinates]:f.geometry.coordinates;for(const poly of polys)for(const ring of poly)ring.reverse();}if(geoArea(f)>0.1)throw new Error('D3 complement polygon');}
 const projection=geoMercator().fitSize([800,800],d3geo),path=geoPath(projection);if(d3geo.features.some(f=>!path(f)||/NaN|Infinity/.test(path(f))))throw new Error('D3 path math invalid');
 await writeFile(out+'/taxi-zones-d3.geojson',JSON.stringify(d3geo)+'\n');await writeFile(out+'/zone-groups.json',JSON.stringify(mapping,null,2)+'\n');await writeFile(out+'/zone-groups.csv',csvFormat(mapping)+'\n');
 const summary={quality,official_cbd_count:ids.length,official_cbd_ids:ids,groups:Object.fromEntries([...new Set(mapping.map(r=>r.zone_group))].map(g=>[g,mapping.filter(r=>r.zone_group===g).map(r=>r.LocationID)])),relations:Object.fromEntries([...new Set(mapping.map(r=>r.polygon_relation).filter(Boolean))].map(g=>[g,mapping.filter(r=>r.polygon_relation===g).map(r=>r.LocationID)])),rule:'Official MTA taxi CBD membership; nonmember Manhattan shared edge>1foot north candidates; Brooklyn/Queens polygon distance<=500m across-river candidates. Central Park ambiguous. Adjacency is proposed for review, not official toll classification.',sources:['https://data.ny.gov/d/yfdc-w5jh','https://data.ny.gov/d/srxy-5nxn','https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip'],rendering_check:'D3 spherical area/path math passed; no visual/UI rendering test performed'};
 await writeFile(out+'/validation.json',JSON.stringify(summary,null,2));console.log(JSON.stringify(summary,null,2));
}finally{c.closeSync();db.closeSync()}
