from sqlalchemy import create_engine
import pandas as pd
engine = create_engine('sqllite:///abc.sqlite')
# Open engine in context manager
# Perform query and save results to DataFrame: df
with engine.connect() as con:
    rs = con.execute("SELECT Title, Name FROM Album INNER JOIN Artist on Album.ArtistID = Artist.ArtistID")
    df = pd.DataFrame(rs.fetchall())
    df.columns = rs.keys()

# Print head of DataFrame df
print(df.head())


'''-------------------- Using Pandas Only ---------------------------------------------'''
# Execute query and store records in DataFrame: df
df = pd.read_sql_query(
    '''SELECT * FROM PlaylistTrack INNER JOIN Track 
    ON PlaylistTrack.TrackId = Track.TrackId WHERE Milliseconds < 250000 ''',
    engine
)

# Print head of DataFrame
print(df.head())
