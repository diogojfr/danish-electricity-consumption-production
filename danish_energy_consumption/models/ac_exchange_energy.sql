{{ config(materialized='table') }}

WITH ac_energy_exchange AS (
    SELECT 	date_time,
			exchange_norway_mwh,
			exchange_sweden_mwh,
			exchange_germany_mwh,
			exchange_netherlands_mwh,
			exchange_great_bretain_mwh
    FROM {{ ref("ac_total_table") }}
)

SELECT * 
FROM ac_energy_exchange
/*
WITH wind_energy AS (
	SELECT offshorewindlt100mw_mwh,
			offshorewindge100mw_mwh,
			onshorewindlt50kw_mwh,
			onshorewindge50kw_mwh
	FROM danish_electricity_total
)

SELECT *
FROM wind_energy


WITH temp_table AS (
	SELECT *
	FROM danish_electricity_total
)

SELECT DISTINCT hourutc AS date_time,
		offshorewindlt100mw_mwh AS offshore_wind_lt100mw_mwh,
		offshorewindge100mw_mwh AS offshore_wind_ge100mw_mwh,
		onshorewindlt50kw_mwh AS onshore_wind_lt50kw_mwh,
		onshorewindlt50kw_mwh AS,
		onshorewindge50kw_mwh AS,
		hydropowermwh AS,
		solarpowerlt10kw_mwh AS,
		solarpowerge10lt40kw_mwh AS,
		solarpowerge40kw_mwh AS,
		solarpowerselfconmwh AS,
		unknownprodmwh AS ,
FROM temp_table
*/