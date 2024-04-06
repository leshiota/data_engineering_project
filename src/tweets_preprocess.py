import pandas as pd 
import numpy as np 
import intertools

# load the data
tweeter_path = '/home/leshiota/Documents/data_engineer/projects/azure_project/data/raw/tweeters/hurricane_harvey.csv'

def read_reprocess_tweeters(tweeter_path):
    tweets_df = (pd.read_csv(tweeter_path, encoding='latin1')
                    .drop(labels=['Unnamed: 0'], axis='columns')
                    .dropna()
                    .drop(['ID'],axis='columns')
                    .sample(frac=1).reset_index(drop='True')
    )
    
    tweets_df['Time'] = pd.to_datetime(tweets_df['Time'])

    tweets_df = tweets_df.assign(
        Year=tweets_df['Time'].dt.year,
        Month=tweets_df['Time'].dt.month,
        Day=tweets_df['Time'].dt.day
    )

    tweets_df = tweets_df.assign(
        YearMonthDay = tweets_df['Year'].astype(str) + "-" + tweets_df['Month'].astype(str) + "-" + tweets_df['Day'].astype(str)    
    )

    return tweets_df



def create_groups(group_size, df):

    number_groups = len(df) // group_size
    remainder_from_groups = len(df) % group_size

    #Create artificial ID
    twitter_accounts = []
    for id in range(1, number_groups + 1):
        twitter_accounts.append(
            [number for number in intertools.repeat(id, group_size)])

    for id in range(number_groups, remainder_from_groups +1):
        twitter_accounts.append(
            [number for number in intertools.repeat(id,remainder_from_groups)])

    reminder_ids = [number for number in intertools.repeat(
        number_of_groups +1,  remainder_from_groups)]

    twitter_accounts.append(reminder_ids)    
    


df = read_reprocess_tweeters(tweeter_path)
print(df)


