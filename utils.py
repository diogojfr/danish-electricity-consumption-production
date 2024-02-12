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
  CREATE TABLE daily_historic_pollution (date DATE PRIMARY KEY,
                         type VARCHAR(20),
                         value INTEGER
                         )
  """)
print("Created table")
cur.close()
conn.close()
 
# try 2
# engine = create_engine(connection_string)
# df = pd.read_csv("C:/Users/Administrador/Documents/data-science/portifolio-projects/danish-electricity-consumption-and-production/artifacts/raw_tables/raw_data.csv")
# df.to_sql('danish_electricity_total', con=engine)

# try 3
# up.uses_netloc.append("postgres")
# url = up.urlparse(os.environ["postgres://lsprvksb:jf8w1w_ahrthiX2q7oNfb7nKoUnrjoXf@silly.db.elephantsql.com/lsprvksb"])
# conn = psycopg2.connect(database = url.path[1:],
#                         user = url.username,
#                         password = url.password,
#                         host = url.hostname,
#                         port = url.port)
df = pd.read_csv("C:/Users/Administrador/Documents/data-science/portifolio-projects/danish-electricity-consumption-and-production/artifacts/raw_tables/raw_data.csv")
#df.to_sql('danish_electricity_total', conn)

db = create_engine(connection_string)
conn = db.connect()

df.to_sql('danish_electricity_total', con=conn, if_exists='replace',
          index=False)

conn = psycopg2.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
query_test = '''select * from danish_electricity_total;'''
cursor.execute(query_test)
#for i in cursor.fetchall():
conn.close()