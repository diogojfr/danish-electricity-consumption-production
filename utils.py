import psycopg2
import psycopg2.extras
import sys
import os
from src.exception import CustomException
from src.logger import logging

def loading_data(hostname, database, username, pwd, port_id, df):
    conn = None
    try:
         with psycopg2.connect(
             host = hostname,
             dbname = database,
             user = username,
             password = pwd,
             port = port_id) as conn:

             with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                cur.execute(
                    """
                    CREATE SCHEMA IF NOT EXISTS data_source
                    """)
                
                cur.execute(
                    """
                    CREATE SCHEMA IF NOT EXISTS transformations
                    """)
                
                cur.execute(
                    """
                    DROP TABLE IF EXISTS data_source.danish_electricity_total
                    """)
                
                cur.execute(
                    """
                    CREATE TABLE data_source.danish_electricity_total(HourUTC TIMESTAMP PRIMARY KEY,
                                            PriceArea VARCHAR(10),
                                            CentralPowerMWh FLOAT(2), 
                                            LocalPowerMWh FLOAT(2), 
                                            CommercialPowerMWh FLOAT(2), 
                                            LocalPowerSelfConMWh FLOAT(2), 
                                            OffshoreWindLt100MW_MWh FLOAT(2), 
                                            OffshoreWindGe100MW_MWh FLOAT(2), 
                                            OnshoreWindLt50kW_MWh FLOAT(2),
                                            OnshoreWindGe50kW_MWh FLOAT(2), 
                                            HydroPowerMWh FLOAT(2), 
                                            SolarPowerLt10kW_MWh FLOAT(2),
                                            SolarPowerGe10Lt40kW_MWh FLOAT(2), 
                                            SolarPowerGe40kW_MWh FLOAT(2),
                                            SolarPowerSelfConMWh FLOAT(2), 
                                            UnknownProdMWh FLOAT(2), 
                                            ExchangeNO_MWh FLOAT(2),
                                            ExchangeSE_MWh FLOAT(2), 
                                            ExchangeGE_MWh FLOAT(2), 
                                            ExchangeNL_MWh FLOAT(2), 
                                            ExchangeGB_MWh FLOAT(2),
                                            ExchangeGreatBelt_MWh FLOAT(2), 
                                            GrossConsumptionMWh FLOAT(2),
                                            GridLossTransmissionMWh FLOAT(2), 
                                            GridLossInterconnectorsMWh FLOAT(2),
                                            GridLossDistributionMWh FLOAT(2), 
                                            PowerToHeatMWh FLOAT(2));""")
                
                for i in range(len(df)):
                    querys = """INSERT INTO data_source.danish_electricity_total(HourUTC, PriceArea, CentralPowerMWh, LocalPowerMWh, 
                                                        CommercialPowerMWh, LocalPowerSelfConMWh, OffshoreWindLt100MW_MWh,     
                                                        OffshoreWindGe100MW_MWh, OnshoreWindLt50kW_MWh, OnshoreWindGe50kW_MWh, 
                                                        HydroPowerMWh, SolarPowerLt10kW_MWh,SolarPowerGe10Lt40kW_MWh, SolarPowerGe40kW_MWh, 
                                                        SolarPowerSelfConMWh, UnknownProdMWh, ExchangeNO_MWh, ExchangeSE_MWh, ExchangeGE_MWh, 
                                                        ExchangeNL_MWh, ExchangeGB_MWh, ExchangeGreatBelt_MWh, 
                                                        GrossConsumptionMWh, GridLossTransmissionMWh, GridLossInterconnectorsMWh,
                                                        GridLossDistributionMWh, PowerToHeatMWh) 
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                    cur.execute(querys, (df.HourUTC[i], df.PriceArea[i], df.CentralPowerMWh[i], df.LocalPowerMWh[i], 
                                        df.CommercialPowerMWh[i],df.LocalPowerSelfConMWh[i], df.OffshoreWindLt100MW_MWh[i], 
                                        df.OffshoreWindGe100MW_MWh[i], 
                                        df.OnshoreWindLt50kW_MWh[i],df.OnshoreWindGe50kW_MWh[i], df.HydroPowerMWh[i], df.SolarPowerLt10kW_MWh[i],
                                        df.SolarPowerGe10Lt40kW_MWh[i], df.SolarPowerGe40kW_MWh[i],df.SolarPowerSelfConMWh[i], df.UnknownProdMWh[i], 
                                        df.ExchangeNO_MWh[i], df.ExchangeSE_MWh[i], df.ExchangeGE_MWh[i], df.ExchangeNL_MWh[i], df.ExchangeGB_MWh[i], 
                                        df.ExchangeGreatBelt_MWh[i], df.GrossConsumptionMWh[i], df.GridLossTransmissionMWh[i], 
                                        df.GridLossInterconnectorsMWh[i], 
                                        df.GridLossDistributionMWh[i], df.PowerToHeatMWh[i]))                

                             
    except Exception as e:
        raise CustomException(sys, e)
    
    finally:
        if conn is not None:
            conn.close()

