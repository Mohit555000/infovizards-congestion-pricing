export function stationChange(base,post,{identityValid=true}={}){
 const status=base===null?'missing_baseline':post===null?'missing_post':!identityValid?'uncertain_station_identity':base===0?'zero_baseline':base<100?'low_volume':'valid';
 return {baseline_status:base===null?'not_observed_2024':base===0?'observed_zero':'observed_positive',comparison_status:status,low_volume:base===null?null:base<100,pct_change:status==='valid'?100*(post-base)/base:null,new_station_confirmed:null};
}
export const coordValid=(lon,lat)=>Number.isFinite(lon)&&Number.isFinite(lat)&&Math.abs(lon)<=180&&Math.abs(lat)<=90;
