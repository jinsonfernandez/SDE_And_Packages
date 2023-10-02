# Slicing and
# subsetting with .loc
# and .iloc

import numpy as np
import pandas as pd

dogs = pd.read_csv("dogs.csv")
vet_visits = pd.read_csv('vet_visits.csv')

#Setting Index
dogs_srt = dogs.set_index(["breed", "color"]).sort_index()

# Slicing the outer index level
dogs_srt.loc["Chow Chow":"Poodle"]

#Slicing the inner index levels
dogs_srt.loc[("Labrador", "Brown"):("Schnauzer", "Grey")]

#Slicing columns
dogs_srt.loc[:, "name":"height_cm"]

#slicing rows and columns
dogs_srt.loc[
("Labrador", "Brown"):("Schnauzer", "Grey"),
"name":"height_cm"]

# Setting dates as index
dogs = dogs.set_index("date_of_birth").sort_index()
dogs.loc["2014-08-25":"2016-09-16"]

# Slicing by partial dates
dogs.loc["2014":"2016"]

# Pivoting + loc
dogs_height_by_breed_vs_color = dog_pack.pivot_table(
"height_cm", index="breed", columns="color")

dogs_height_by_breed_vs_color.loc["Chow Chow":"Poodle"]

dogs_height_by_breed_vs_color.mean(axis="index")


# Calculating summary stats across columns
dogs_height_by_breed_vs_color.mean(axis="columns")

