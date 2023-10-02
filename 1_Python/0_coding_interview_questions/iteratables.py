import pandas as pd

pars = {'weight': [168, 183, 198], 'height': [77, 79, 135]}
characters = pd.DataFrame(pars, index=['Luke Skywalker', 'Han Solo', 'Darth Vader'])

for index, series in characters.iterrows():
    print(f"Index is :{index}")
    print(f"Series is: {series} ")
print('------------------------------------------------------------')

for name, series in characters.items():
    print(name)
    print(series)
print('------------------------------------------------------------')

nums_new = []
for i in range(1, 6):
    nums_new.append(2 * i)
print(nums_new)

# Using lst comprehension
new_nums = [2 * i for i in range(1, 6)]
print(new_nums)

# Lst comprehension with condition
new_nums1 = [num for num in range(1, 11) if num % 2 == 0]
print(new_nums1)
print('------------------------------------------------------------')

numbers = [1, 2, 3]
letters = ['a', 'b', 'c']

''' Find all Possible combinations betweeen the 2 lists'''

# Approach 1
pairs = []
for i in numbers:
    for j in letters:
        pairs.append((i, j))
print(pairs)

# Approach 2
pairs = [(i, j) for i in numbers for j in letters]
print(pairs)

# Approach 3 using itertools
import itertools
pairs_1 = list(itertools.product(numbers, letters))
print(pairs_1)
print('------------------------------------------------------------')

# Swap numbers and letters
swapped = [[(j,i) for i in numbers] for j in letters]
print(swapped)

# getting the count of each element
count_a = sum(item.count('a') for sublist in swapped for item in sublist)
print("Count of 'a':", count_a)
print('------------------------------------------------------------')

'''------------------------------------------------- ZIP ----------------------------------------------------------'''
# zip - object that combines several iterable objects into one iterable object

title = 'TMNT'
villains = ['Shredder', 'Krang', 'Bebop', 'Rocksteady']
turtles = {
'Raphael': 'Sai', 'Michelangelo': 'Nunchaku',
'Leonardo': 'Twin katana', 'Donatello': 'Bo'
}

result = zip(title,villains, turtles)
for i in result:
    print(i)
    print(list(i))


# Reverse Operation
turtle_masks = [
('Raphael', 'red'), ('Michelangelo', 'orange'),
('Leonardo', 'blue'), ('Donatello', 'purple')
]
result_1 = zip(*turtle_masks)
print(result_1)

# A zip object can be used to create a dictionary
keys = ['movie', 'year', 'director']
values = [
['Forest Gump', 'Goodfellas', 'Se7en'],
[1994, 1990, 1995],
['R.Zemeckis', 'M.Scorsese', 'D.Fincher']
]

movies = dict(zip(keys, values))
print(movies)
df_movies = pd.DataFrame(movies)
#list() → zip() → dict()  → DataFrame()

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


# Iterate over the dictionary and print each key-value pair:
for key, value in dictionary.items():
    print(key, value)

