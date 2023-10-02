import numpy as np
import pandas as pd

# num_array = np.array([1, 2, 3, 4, 5])
# print(num_array)

'''Acessing elements list vs  Numpy Array'''
list2d = [
    [1,2,3,4,5],
    [6,7,8,9,10],
    [11,12,13,14,15]
]

#retrive [[2,3,4],[789]]
# using normal loop
result = []
for j in list2d[0:2]:
    subresult = []
    for i in range(len(j)-2):
        subresult.append(j[i:i+3])
    result.append(subresult)
print(result)

# Above statement using list comrehension
result = [ [j[i:i+3] for i in range(len(j)-2)]   for i in list2d[0:2]  ]

# A more simpler way to get the same result
print([
    [list2d[j][1:4] for j in range(0,2)]
])

'''============= Using numpy array =============='''
array2d = np.array([
    [1,2,3,4,5],
    [6,7,8,9,10],
    [11,12,13,14,15]
])

result = array2d[0:2,1:4]
print(f"Numpy result is \n{result}")
print('-----------------------')
print(array2d[0:2,0])


print('-----------------------')
