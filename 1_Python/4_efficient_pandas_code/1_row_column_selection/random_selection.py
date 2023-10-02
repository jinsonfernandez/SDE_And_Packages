from datetime import time
# Extract number of rows in dataset
N=poker_hands.shape[0]
# Extract number of columns in dataset
D=poker_hands.shape[1]

# Select and time the selection of the 75% of the dataset's rows
rand_start_time = time.time()
poker_hands.iloc[np.random.randint(low=0, high=N, size=int(0.75 * N))]

print("Time using Numpy: {} sec".format(time.time() - rand_start_time))

# Select and time the selection of 4 of the dataset's columns using NumPy
np_start_time = time.time()
poker_hands.iloc[:,np.random.randint(low=0, high=D, size=4)]
print("Time using NymPy's random.randint(): {} sec".format(time.time() - np_start_time))