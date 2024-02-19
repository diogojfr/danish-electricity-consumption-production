import psycopg2
import urllib.parse as up
import os
import pandas as pd
import io
from sqlalchemy import create_engine

connection_string = "postgres://user:password@silly.db.elephantsql.com/user"
conn = psycopg2.connect(connection_string)
conn.set_session(autocommit=True)
cur = conn.cursor()

cur.execute(
   """
   DROP TABLE IF EXISTS danish_electricity_total
   """)

cur.execute(
  """
  CREATE TABLE danish_electricity_total (HourUTC TIMESTAMP PRIMARY KEY,
                         HourDK TIMESTAMP,
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
                         PowerToHeatMWh FLOAT(2)                      
                         )
  """)
print("Created table")
cur.close()
conn.close()

df = pd.read_csv("C:/Users/Administrador/Documents/data-science/portifolio-projects/danish-electricity-consumption-and-production/artifacts/raw_tables/raw_data.csv")
 
engine = create_engine(connection_string)

for i in range(len(df)):
    query = """ INSERT INTO danish_electricity_total (HourUTC, HourDK, PriceArea,       CentralPowerMWh, LocalPowerMWh, CommercialPowerMWh, LocalPowerSelfConMWh, OffshoreWindLt100MW_MWh, OffshoreWindGe100MW_MWh, OnshoreWindLt50kW_MWh,
    OnshoreWindGe50kW_MWh, HydroPowerMWh, SolarPowerLt10kW_MWh,
    SolarPowerGe10Lt40kW_MWh, SolarPowerGe40kW_MWh,
    SolarPowerSelfConMWh, UnknownProdMWh, ExchangeNO_MWh,
    ExchangeSE_MWh, ExchangeGE_MWh, ExchangeNL_MWh, ExchangeGB_MWh,
    ExchangeGreatBelt_MWh, GrossConsumptionMWh,
    GridLossTransmissionMWh, GridLossInterconnectorsMWh,
    GridLossDistributionMWh, PowerToHeatMWh) VALUES (%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s);
    """
    engine.execute(query, df.HourUTC[i], df.HourDK[i], df.PriceArea[i],
                   df.CentralPowerMWh[i], df.LocalPowerMWh[i], df.CommercialPowerMWh[i],df.LocalPowerSelfConMWh[i], )
