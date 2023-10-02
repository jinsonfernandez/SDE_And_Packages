import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
marketing = pd.read_csv('marketing.csv')

daily_users = marketing.groupby(['date_served'])['user_id'].nunique()
# Plot
daily_users.plot()
# Annotate
plt.title('Daily number of users who see ads')
plt.xlabel('Date')
plt.ylabel('Number of users')
plt.xticks(rotation = 45)
plt.show()


# Conversion rate = Total number of people we marketed to / Number of people who convert
subscribers = marketing[marketing['converted'] == True]['user_id'].nunique()
total = marketing['user_id'].nunique()
conv_rate = subscribers/total
print(round(conv_rate*100, 2),'%')

# Retention Rate = Number of people who remain subscribed/Total number of people who converted
retained = marketing[marketing['is_retained'] == True]['user_id'].nunique()
subscribers = marketing[marketing['converted'] == True]['user_id'].nunique()
retention = retained/subscribers
print(round(retention*100, 2),'%')

#Segmentation - GroupBy
# Group by subscribing_channel and calculate retention
retained = marketing[marketing['is_retained'] == True].groupby(['subscribing_channel'])['user_id'].nunique()

# Daily retention rate
total = marketing.groupby(['date_subscribed'])['user_id'].nunique()
retained = marketing[marketing['is_retained']==True].groupby(['date_subscribed'])['user_id'].nunique()
daily_retention_rate = retained/total

'''================================================================================================================='''
# Grouping by multiple columns
language = marketing.groupby(['date_served','language_preffered'])['user_id'].count()

#unstacking after grouping
language = pd.DataFrame(language.unstack(level=1))

