import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json
from pandas.io.json import json_normalize


engine = create_engine('postgresql://mmooney@gispgdb.nrel.gov:5432/dav-gis')
sql = 'select rate_id_alias, cast(json as text) from diffusion_data_shared.urdb_rates_sam_min_max'
df = pd.read_sql(sql, engine)


tou = []
kwh_min, kwh_max  = [], []
kw_min, kw_max = [], []
for i,ii in enumerate (df['json']):
		x = json_normalize(json.loads(df['json'][i]))
		tou.append(x['d_tou_exists'][0])
		kw_min.append(x['peak_kW_capacity_min'][0])
		kw_max.append(x['peak_kW_capacity_max'][0])
		kwh_min.append(x['kWh_useage_min'][0])
		kwh_min.append(x['kWh_useage_max'][0])




df2 = pd.DataFrame({'rate_id_alias': df['rate_id_alias'].values, 'tou': tou, 'max_demand_kw': kw_max,
                    'min_demand_kw': kw_min, 'max_energy_kwh': kwh_max, 'min_energy_kwh': kwh_min})


df2.to_sql('urdb_rates_sam_min_max', engine, 'pgsql', 'diffusion_shared', index = False)
