{{ config(materialized='view') }}

WITH avg_annual_production AS (
SELECT date_time,
	   hydro_power_mwh,
		(offshore_wind_lt100mw_mwh+
		 offshore_wind_ge100mw_mwh+
		 onshore_wind_lt50kw_mwh+
		 onshore_wind_ge50kw_mwh) AS wind_power_mwh,
		(solar_power_lt10kw_mwh+
		 solar_power_ge10lt40kw_mwh+
		 solar_power_ge40kw_mwh+
		 solar_power_selfcon_mwh) AS solar_power_mwh,
		unknown_prod_mwh
FROM 
	{{ ref("ac_energy_production") }}
ORDER BY 
	date_time DESC	
)


select 
	AVG(hydro_power_mwh) AS hydro,
	AVG(wind_power_mwh) AS wind,
	AVG(solar_power_mwh) AS solar,
	AVG(unknown_prod_mwh) AS unknown
from 
	avg_annual_production