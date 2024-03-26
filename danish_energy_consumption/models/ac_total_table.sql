{{ config(materialized='table') }}

WITH ac_electricity_total AS (
	SELECT *
	FROM {{source ("data_source", "danish_electricity_total") }} 
)

SELECT DISTINCT hourutc AS date_time,
		centralpowermwh AS central_power_mwh,
		localpowermwh AS local_power_mwh,
		localpowerselfconmwh AS local_power_selfcon_mwh,
		hydropowermwh AS hydro_power_mwh,
		offshorewindlt100mw_mwh AS offshore_wind_lt100mw_mwh,
		offshorewindge100mw_mwh AS offshore_wind_ge100mw_mwh,
		onshorewindlt50kw_mwh AS onshore_wind_lt50kw_mwh,
		onshorewindge50kw_mwh AS onshore_wind_ge50kw_mwh,
		solarpowerlt10kw_mwh AS solar_power_lt10kw_mwh,
		solarpowerge10lt40kw_mwh AS solar_power_ge10lt40kw_mwh,
		solarpowerge40kw_mwh AS solar_power_ge40kw_mwh,
		solarpowerselfconmwh AS solar_power_selfcon_mwh,
		unknownprodmwh AS unknown_prod_mwh,
		exchangeno_mwh AS exchange_norway_mwh,
		exchangese_mwh AS exchange_sweden_mwh,
		exchangege_mwh AS exchange_germany_mwh,
		exchangenl_mwh AS exchange_netherlands_mwh,
		exchangegb_mwh AS exchange_great_bretain_mwh,
		grossconsumptionmwh AS gross_consumption_mwh,
		gridlosstransmissionmwh AS grid_loss_transmission_mwh,
		gridlossinterconnectorsmwh AS grid_loss_interconnectors_mwh,
		gridlossdistributionmwh AS grid_loss_distribution_mwh,
		powertoheatmwh AS power_to_heat_mwh
FROM ac_electricity_total
WHERE hourutc IS NOT NULL