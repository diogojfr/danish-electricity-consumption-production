import psycopg2
import pandas as pd
from sqlalchemy import create_engine

connection_string = "postgres://user:password@silly.db.elephantsql.com/user"
conn = psycopg2.connect(connection_string)

#conn.set_session(autocommit=True)
# cur = conn.cursor()

# cur.execute(
#   """
#   DROP TABLE IF EXISTS daily_historic_pollution
#   """)

# cur.execute(
#   """
#   CREATE TABLE daily_historic_pollution (date DATE PRIMARY KEY,
#                          type VARCHAR(20),
#                          value INTEGER
#                          )
#   """)
# print("Created table")
#cur.close()

#conn.close()
# create_engine = asdas
 
engine = create_engine(connection_string)

conn = engine.connect()

df = pd.read_csv("C:/Users/Administrador/Documents/data-science/portifolio-projects/danish-electricity-consumption-and-production/artifacts/raw_tables/raw_data.csv")

df.to_sql('danish_electricity_total', engine)
