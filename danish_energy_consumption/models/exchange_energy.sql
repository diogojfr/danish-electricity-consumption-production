{{ config(materialized='view') }}

WITH exchange_energy AS (
    SELECT  HourUTC,
            ExchangeNO_MWh,
            ExchangeSE_MWh,
            ExchangeGE_MWh, 
            ExchangeNL_MWh, 
            ExchangeGB_MWh,
            ExchangeGreatBelt_MWh 
    FROM danish_electricity_total
) 

SELECT
        SUM(ExchangeNO_MWh) AS total_exchange_NO,
        SUM(ExchangeSE_MWh) AS total_exchange_SE,
        SUM(ExchangeGE_MWh) AS total_exchange_GE, 
        SUM(ExchangeNL_MWh) AS total_exchange_NL, 
        SUM(ExchangeGB_MWh) AS total_exchange_GB,
        SUM(ExchangeGreatBelt_MWh) AS total_exchange_greatbelt 
FROM exchange_energy