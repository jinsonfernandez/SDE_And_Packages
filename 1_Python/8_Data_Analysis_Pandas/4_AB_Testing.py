import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
marketing = pd.read_csv('marketing.csv')

email = marketing[marketing['marketing_channel'] == 'Email']
allocation = email.groupby(['variant'])\
['user_id'].nunique()
allocation.plot(kind='bar')
plt.title('Personalization test allocation')
plt.xticks(rotation = 0)
plt.ylabel('# participants')
plt.show()

# Group by user_id and variant
subscribers = email.groupby(['user_id',
'variant'])['converted'].max()
subscribers = pd.DataFrame(subscribers.unstack(level=1))

# Drop missing values from the control column
control = subscribers['control'].dropna()
# Drop missing values from the personalization column
personalization = subscribers['personalization'].dropna()

#Lift & Significance testing
# calculating lift = (Treatment conversion rate - Control Conversion rate) / control conversion rate
