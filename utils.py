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
  CREATE TABLE danish_electricity_total (date DATE PRIMARY KEY,
                         type VARCHAR(20),
                         value INTEGER
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
    GridLossDistributionMWh, PowerToHeatMWh)
    
    """

