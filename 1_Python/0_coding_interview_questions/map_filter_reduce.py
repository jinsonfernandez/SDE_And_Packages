# map(function(x1,x2), iterable1, iterable2)

nums = [1, 2, 3, 4, 5]
def squared(x):
    return x**2
squar = map(squared, nums)

# Using lamdas
squared = map(lambda x: x**2, nums)

nums1 = [1, 2, 3, 4, 5]
nums2 = [10, 20, 30, 40, 50]
mult = map(lambda x, y: x*y, nums1, nums2)
list(mult)
'''------------------------------------------------  FILTER ----------------------------------------------------'''

# filter(function(x), Iterable)
nums = [-3, -2, -1, 0, 1, 2, 3]
positive = filter(lambda x: x>0, nums)
list(positive)

'''------------------------------------------------  REDUCE ----------------------------------------------------'''
from functools import reduce
# reduce(function(x, y), Iterable)

nums = [8, 4, 5, 1, 9]
smallest = filter(lambda x, y: x if x < y else y, nums)

'''--------------------------------------------------------------------------------------------------------------'''

from functools import reduce

# Lambda function using list comprehension
process_data = lambda data: [x ** 2 for x in data if x % 2 == 0]

# Sample data
numbers = [1, 2, 3, 4, 5, 6]

# Using the lambda function with map()
mapped_result = list(map(lambda x: x ** 2, numbers))

# Using the lambda function with reduce()
reduced_result = reduce(lambda x, y: x + y, numbers)

# Using the lambda function with filter()
filtered_result = list(filter(lambda x: x % 2 == 0, numbers))

# Printing the results
print("Using map():", mapped_result)
print("Using reduce():", reduced_result)
print("Using filter():", filtered_result)
