import pandas as pd

ride_sharing = pd.read_csv('ride_sharing.csv')

# Find duplicates
'''
keep='first': Marks all duplicates as True except for the first occurrence. Subsequent occurrences of the duplicated values will be marked as False.
keep='last': Marks all duplicates as True except for the last occurrence. The first occurrences of the duplicated values will be marked as False.
keep=False: Marks all duplicates as True. No occurrence of a duplicated value will be marked as False.
'''
duplicates = ride_sharing.duplicated(subset='ride_id', keep=False)

# Sort your duplicated rides vy creating a new dataframe where all records whcih corrospods to True from previous statement are kept
duplicated_rides = ride_sharing[duplicates].sort_values('ride_id')

# Print relevant columns
print(duplicated_rides[['ride_id', 'duration', 'user_birth_year']])

# Drop complete duplicates from ride_sharing
ride_dup = ride_sharing.drop_duplicates()

# Create statistics dictionary for aggregation function
statistics = {'user_birth_year': 'min', 'duration': 'mean'}

# Group by ride_id and compute new statistics
ride_unique = ride_dup.groupby('ride_id').agg(statistics).reset_index()

# Find duplicated values again
duplicates = ride_unique.duplicated(subset = 'ride_id', keep = False)
duplicated_rides = ride_unique[duplicates == True]

# Assert duplicates are processed
assert duplicated_rides.shape[0] == 0
