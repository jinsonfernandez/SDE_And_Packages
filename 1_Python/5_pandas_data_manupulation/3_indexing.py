import numpy as np
import pandas as pd

dogs = pd.read_csv("dogs.csv")
vet_visits = pd.read_csv('vet_visits.csv')

# Setting a column as the index
dogs_ind = dogs.set_index("name")

# Removing an index
dogs_ind.reset_index()

#Dropping an index
dogs_ind.reset_index(drop=True)

# Indexes make subsetting simpler
dogs[dogs["name"].isin(["Bella", "Stella"])]

dogs_ind = dogs.set_index("name")
dogs_ind.loc[["Bella", "Stella"]]

dogs_ind3 = dogs.set_index(["breed", "color"])
dogs_ind3.loc[["Labrador", "Chihuahua"]]

# Subset inner levels with a list of tuples
dogs_ind3.loc[[("Labrador", "Brown"), ("Chihuahua", "Tan")]]

# Sorting Index
dogs_ind3.sort_index(level=["color", "breed"], ascending=[True, False])




