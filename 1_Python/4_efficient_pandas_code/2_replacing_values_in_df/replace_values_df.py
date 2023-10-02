# Replace Class 1 to -2
poker_hands['Class'].replace(1, -2, inplace=True)
# Class 2 to -3
poker_hands['Class'].replace(2, -3, inplace=True)

print(poker_hands[['Class', 'Explanation']])

'''------------------------- Replace scalar values II ------------------------------'''
start_time = time.time()

# Replace all the entries that has 'FEMALE' as a gender with 'GIRL'
names['Gender'].loc[names['Gender'] == 'FEMALE'] = 'GIRL'

print("Time using .loc[]: {} sec".format(time.time() - start_time))

# Better way
names['Gender'].replace('FEMALE', 'GIRL', inplace=True)
print("Time using .replace(): {} sec".fomrat(time.time() - start_time))

# Replace multiple values with one value
start_time = time.time()
names['Ethnicity'].loc[(names["Ethnicity"] == 'WHITE NON HISPANIC') | (names["Ethnicity"] == 'WHITE NON HISP')] = 'WNH'
print("Results from the above operation calculated in %s seconds" %
(time.time() - start_time))

#Better way using replace
names['Ethnicity'].replace(['WHITE NON HISPANIC','WHITE NON HISP'], 'WHN',inplace = True)

names['Ethnicity'].replace(['BLACK NON HISP', 'WHITE NON HISP'], ['BLACK NON HISPANIC', 'WHITE NON HISPANIC'], inplace=True)