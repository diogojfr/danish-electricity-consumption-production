{{ config(materialized='table') }}

WITH ac_energy_production AS (
    SELECT 	date_time,
			hydro_power_mwh,
			offshore_wind_lt100mw_mwh,
		    offshore_wind_ge100mw_mwh,
		    onshore_wind_lt50kw_mwh,
		    onshore_wind_ge50kw_mwh,
			solar_power_lt10kw_mwh,
			solar_power_ge10lt40kw_mwh,
			solar_power_ge40kw_mwh,
			solar_power_selfcon_mwh,
			unknown_prod_mwh
    FROM {{ ref("ac_total_table") }}
)

SELECT * 
FROM ac_energy_production