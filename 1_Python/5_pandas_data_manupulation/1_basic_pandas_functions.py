import numpy as np
import pandas as pd

dogs = pd.read_csv("dogs.csv")
                                                # Sorting
dogs.sort_values(["weight_kg", "height_cm"], ascending=[True, False])


                                                # Subsetting
dogs[["breed", "height_cm"]]

# Subsetting rows
dogs[dogs["height_cm"] > 50]
dogs[dogs["breed"] == "Labrador"]

dogs[dogs["date_of_birth"] < "2015-01-01"]

dogs[ (dogs["breed"] == "Labrador") & (dogs["color"] == "Brown") ]
is_black_or_brown = dogs["color"].isin(["Black", "Brown"])
dogs[is_black_or_brown]

                                                # Adding a new columns

dogs["height_m"] = dogs["height_cm"] / 100
dogs["bmi"] = dogs["weight_kg"] / dogs["height_m"] ** 2