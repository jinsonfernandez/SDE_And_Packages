import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
engine = create_engine('sqllite:///abc.sqlite')
# Save the table names to a list: table_names
table_names = engine.table_names()
# Print the table names to the shell
print(table_names)
con = engine.connect()
rs = con.execute("Select * from orders")
df = pd.DataFrame(rs.fetchall())
con.close()

print(df.head())

'''---------------------- Customizing SQL Fetch ------------------------'''
engine = create_engine('sqlite:///Chinook.sqlite')
# Perform query and save results to DataFrame: df

with engine.connect() as con:
    rs = con.execute("SELECT LastName, Title FROM Employee")
    rs1 = con.execute("SELECT * FROM Employee WHERE EmployeeId >= 6")
    df = pd.DataFrame(rs.fetchmany(size=3))
    df1 = df = pd.DataFrame(rs1.fetchall())
    df.columns = rs.keys()
    df1.columns = rs1.keys()

# Print the length of the DataFrame df
print(len(df))

# Print the head of the DataFrame df
print(df.head())

'''-------------------------  Using pandas read_sql_query ------------------------------------------'''
# Create engine: engine
engine = create_engine('sqlite:///Chinook.sqlite')

# Execute query and store records in DataFrame: df
df = pd.read_sql_query("SELECT * FROM Album", engine)

# Print head of DataFrame
print(df.head())

# Open engine in context manager and store query result in df1
with engine.connect() as con:
    rs = con.execute("SELECT * FROM Album")
    df1 = pd.DataFrame(rs.fetchall())
    df1.columns = rs.keys()

# Confirm that both methods yield the same result
print(df.equals(df1))

