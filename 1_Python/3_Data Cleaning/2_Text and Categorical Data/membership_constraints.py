import pandas as pd
airlines = pd.read_csv('airline.txt')
categories = airlines.iloc[:,-3] # using loc we can do as ride_sharing.loc[:, ['d', 'e', 'f']]

# Find the cleanliness category in airlines not in categories
cat_clean = set(airlines['cleanliness']).difference(categories['cleanliness'])

# Find rows with that category
cat_clean_rows = airlines['cleanliness'].isin(cat_clean)

# Print rows with inconsistent category
print(airlines[cat_clean_rows])

# Print rows with consistent categories only
print(airlines[~cat_clean_rows])