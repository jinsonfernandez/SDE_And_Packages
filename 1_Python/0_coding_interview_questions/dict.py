# A zip object can be used to create a dictionary
keys = ['movie', 'year', 'director']
values = [
['Forest Gump', 'Goodfellas', 'Se7en'],
[1994, 1990, 1995],
['R.Zemeckis', 'M.Scorsese', 'D.Fincher']
]

dictionary = dict(zip(keys, values))


'''
Accessing values:

Access the list of movies: movies = dictionary['movie']
Access the list of years: years = dictionary['year']
Access the list of directors: directors = dictionary['director']
Adding new key-value pairs:

Add a new key-value pair for 'genre': dictionary['genre'] = ['Drama', 'Crime', 'Thriller']
Modifying values:

Update the director of the first movie: dictionary['director'][0] = 'R. Zemeckis'
Removing key-value pairs:

Remove the 'year' key-value pair: del dictionary['year']
Checking if a key exists:

Check if 'rating' key exists: if 'rating' in dictionary: ...
Getting the number of items (key-value pairs) in the dictionary:

Get the length of the dictionary: length = len(dictionary)
'''

# Calculate the total number of movies in the dictionary:
total_movies = 0
for key in dictionary:
    total_movies += len(dictionary[key])

# Filter movies released after 1990 and their respective directors:
filtered_data = {}
for i in range(len(dictionary['movie'])):
    if dictionary['year'][i] > 1990:
        filtered_data[dictionary['movie'][i]] = dictionary['director'][i]

# Applying transformations:
# Convert the year to a string representation for each movie:

for i in range(len(dictionary['year'])):
    dictionary['year'][i] = str(dictionary['year'][i])

# Counting occurrences:
# Count the number of movies directed by each director:
director_count = {}
for director in dictionary['director']:
    if director in director_count:
        director_count[director] += 1
    else:
        director_count[director] = 1

# Grouping data:
# Group movies by the year of release:
movies_by_year = {}
for i in range(len(dictionary['movie'])):
    year = dictionary['year'][i]
    if year in movies_by_year:
        movies_by_year[year].append(dictionary['movie'][i])
    else:
        movies_by_year[year] = [dictionary['movie'][i]]
