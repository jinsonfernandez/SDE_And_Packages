import pandas as pd
import numpy as np
'''

select t.a, t,b, y.u, sum(y.k)
from table1 as t left join table2 as y
where (t.a <> 'abc' and t.b = 'xyz) or y.u is not null
group by t.a, t,b, y.u
Order by t.b desc
having sum(y.k) >100
'''
result = pd.merge(table1.rename(columns={'a': 't_a', 'b': 't_b'}), table2, how='left') \
    .query("(t_a != 'abc' and t_b == 'xyz') or y.u.notnull()") \
    .groupby(['t_a', 't_b', 'b', 'y.u']).agg({'y.k': 'sum'}).reset_index() \
    .query("y.k > 100").sort_values('t_b', ascending=False)[['t_a', 't_b', 'b', 'y.u', 'y.k']]

'''==================================================== Slicing =================================================='''

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
