import numpy as np
import pandas as pd

dogs = pd.read_csv("dogs.csv")
vet_visits = pd.read_csv('vet_visits.csv')
dogs["date_of_birth"].min()
dogs["weight_kg"].agg(lambda x: x.quantile(0.3))

                                                # Summaries on multiple columns
dogs["weight_kg"].agg( [lambda x: x.quantile(0.3), lambda x: x.quantile(0.4)] )

                                                    # Dropping duplicate names
unique_dogs = vet_visits.drop_duplicates(subset=["name", "breed"] )
unique_dogs["breed"].value_counts(sort=True)
unique_dogs["breed"].value_counts(normalize=True)

                                                    # Summaries by group
dogs[dogs["color"] == "Black"]["weight_kg"].mean()
dogs.groupby("color")["weight_kg"].mean()
black_dogs = dog_groups = dogs.groupby("color").filter(lambda x: x["color"].iloc[0] == "black").mean()
dogs.groupby(["color", "breed"])[["weight_kg", "height_cm"]].mean()


                                                    #pivoting the table
# Group by to pivot table
dogs.groupby("color")["weight_kg"].mean()
dogs.pivot_table(values="weight_kg", index="color")

dogs.pivot_table(values="weight_kg", index="color", aggfunc=[np.mean, np.median])

dogs.groupby(["color", "breed"])["weight_kg"].mean()
dogs.pivot_table(values="weight_kg", index="color", columns="breed",fill_value=0)

dogs_height_by_breed_vs_color = dog_pack.pivot_table(values="height_cm", index="breed", columns="color")


'''============================================== Indexing ================================================='''
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







