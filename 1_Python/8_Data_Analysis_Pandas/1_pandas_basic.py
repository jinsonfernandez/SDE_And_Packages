import pandas as pd
import numpy as np
marketing = pd.read_csv('marketing.csv')

print(marketing.describe())
print(marketing.info())

# Change the data type of a column
marketing['converted'] = marketing['converted'].astype('bool')

#Creating new boolean columns
marketing['is_house_ads'] = np.where(marketing['marketing_channel'] == 'House Ads',True, False)

#Mapping values to existing columns
channel_dict = {"House Ads": 1, "Instagram": 2, "Facebook": 3, "Email": 4, "Push": 5}
marketing['channel_code'] = marketing['marketing_channel'].map(channel_dict)

# Parse dates while reading file
marketing = pd.read_csv('marketing.csv',parse_dates=['date_served', 'date_subscribed', 'date_canceled'])

# Convert already existing column to datetime column
marketing['date_served'] = pd.to_datetime(marketing['date_served'])
marketing['date_subscribed'] = marketing['date_served'].dt.dayofweek


