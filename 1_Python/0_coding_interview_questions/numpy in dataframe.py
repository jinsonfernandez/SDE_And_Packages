import numpy as np
import pandas as pd


#using .apply
data = {
    'math score': [74, 44, 54, 88, 85],
    'reading score': [86, 49, 46, 95, 81],
    'writing score': [82, 53, 43, 92, 81]
}
data_df=  pd.DataFrame(data)
print(data_df.head())

data_df['total_score'] = data_df.values.sum(axis=1)
print(data_df.head())

sqrt_score = data_df.apply(np.sqrt)
print(sqrt_score.head())

data_df_new = data_df.apply(lambda x: x/2)
print(data_df_new.head())

'''--------------------------------------------------------------------------------------------------------------------'''
# axis=0 - function is applied over columns
# axis=1 - function is applied over rows

scores = data_df
scores_mean_column = scores.apply(np.mean, axis=0)
scores_mean_row = scores.apply(np.mean, axis=1)
print(scores_mean_column.head())
print(scores_mean_row.head())

print('''------------------------------------------------------''')
score_new_column = scores.apply(lambda x: [np.min(x), np.max(x)],result_type='expand',axis=0)
score_new_row = scores.apply(lambda x: [np.min(x), np.max(x)],result_type='expand',axis=0)
print(scores)
print(score_new_column)
print(score_new_row)
