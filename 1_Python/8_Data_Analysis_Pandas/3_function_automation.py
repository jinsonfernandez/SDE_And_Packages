import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
marketing = pd.read_csv('marketing.csv')

def retention_rate(dataframe, column_names):
    # dogs[(dogs["breed"] == "Labrador") & (dogs["color"] == "Brown")]

    retained = dataframe[dataframe['is_retained'] == True].groupby(['column_names'])['user_id'].nunique()
    converted = dataframe[dataframe['converted'] == True].groupby(['column_names'])['user_id'].nunique()
    retention_rate = retained / converted
    return retention_rate

daily_retention = retention_rate(marketing, ['date_subscribed','subscribing_channel'])
daily_retention = pd.DataFrame(daily_retention.unstack(level=1))

# Identifying
# inconsistencies

DoW_retention = retention_rate(marketing, ['DoW'])
house_ads_no_bugs = house_ads[(house_ads['date_served'] <= '2018-01-11') & (house_ads['marketing_channel'] == True)]
pre_retention = retention_rate(house_ads_no_bugs,[''])

language_conversion = house_ads.groupby(['date_served', 'language_preferred']).agg({'user_id':'nunique','converted':'sum'})