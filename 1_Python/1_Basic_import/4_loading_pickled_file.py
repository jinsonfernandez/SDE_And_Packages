import pickle
import pandas as pd

with open('data.pkl', 'rb') as file:
    d= pickle.load(file)
print(d)
print(type(d))

