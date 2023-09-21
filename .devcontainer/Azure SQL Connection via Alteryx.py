from ayx import Alteryx
import pyodbc
import pandas as pd
import datetime

df = Alteryx.read("#1")

# Set the connection details from single row input from Alteryx
server = df.iloc[0]['server']
database = df.iloc[0]['database']
client_id = df.iloc[0]['client_id']
client_secret = df.iloc[0]['client_secret']
tenant_id = df.iloc[0]['tenant_id']

# Create the connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},1433;DATABASE={database};UID={client_id};PWD={client_secret};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryServicePrincipal;'

# Connect to the database
conn = pyodbc.connect(conn_str)

# Execute a query
cursor = conn.cursor()
cursor.execute('SELECT * FROM <table name>' )

# Fetch the results
results = cursor.fetchall()

# Set column names to a list
columns_names = [column[0] for column in cursor.description]

# Convert the payload to a DataFrame
df = pd.DataFrame.from_records(results)

# Set column names for the DataFrame
df.columns = columns_names

# Close the connection
conn.close()

Alteryx.write(df, 1)