import  numpy as np
from datetime import  time

# Calculate the variance in each hand
start_time = time.time()
poker_var = poker_hands[['R1','R2','R3','R4','R5']].values.var(axis=1, ddof=1)
print("Time using NumPy vectorization: {} sec".format(time.time() - start_time))
print(poker_var[0:5])