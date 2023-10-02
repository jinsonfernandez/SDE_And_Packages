import pandas as pd
import numpy as np

data = {
    'age': [64, 76, 38, 40, 72],
    'gender': ['Female', 'Male', 'Female', 'Male', 'Female'],
    'smoking': ['Former', 'Never', 'Former', 'Former', 'Never'],
    'bmi': [21.48380, 23.87631, 20.01080, 25.14062, 20.98504],
    'vitamin use': ['Yes_fairly_often', 'Yes_fairly_often', 'Yes_not_often', 'No', 'Yes_fairly_often'],
    'plasma B-carotene': [200, 124, 328, 153, 92],
    'plasma retinol': [915, 727, 721, 615, 799]
}

retinol = pd.DataFrame(data)


for group in retinol.groupby(['gender', 'smoking']):
    #each group is a tuple
    # first element is the grouping factor
    print(group[0])
    #second element is the data frame
    print(group[1].head(4))

'''---------------------------- Using agg + group by -----------------------------'''
# .agg(function, axis= , args= )

print(f" Mean Plasma Retional Value is : {retinol['plasma retinol'].agg(np.mean)} ")
print(f"  Mean All-Plasma  Value is : \n{retinol[['plasma retinol','plasma B-carotene']].agg( [np.mean, np.std] )} ")

print('''==========================================================================================''')
print(retinol.groupby('gender')[['bmi', 'age']].agg([np.mean, np.std]))


# def n_more_than_mean (series):
#     result = series[series > np.mean(series)]
#     return len (result)

n_more_than_mean = lambda series: len(series[series > np.mean(series)])
retinol.groupby(['gender', 'smoking'])[['plasma B-carotene', 'plasma retinol' ]].agg([n_more_than_mean, lambda x: len (x)])

'''---------------------------- Using filter -----------------------------'''
bmi_filter = retinol.groupby(['gender', 'smoking']).filter(lambda dataframe: np.mean(dataframe['bmi']) > 26)
print(bmi_filter)