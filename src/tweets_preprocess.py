import pandas as pd 
import numpy as np 
import itertools
# load the data
tweeter_path = '/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/raw/tweeters/hurricane_harvey.csv'

def read_preprocess_tweeters(tweeter_path):
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
            [number for number in itertools.repeat(id, group_size)])

    for id in range(number_groups, remainder_from_groups +1):
        twitter_accounts.append(
            [number for number in itertools.repeat(id,remainder_from_groups)])

    reminder_ids = [number for number in itertools.repeat(
        number_groups +1,  remainder_from_groups)]

    twitter_accounts.append(reminder_ids)    
    
    group_id = []
    for sublist in twitter_accounts:
        for element in sublist:
            group_id.append(element)

    return group_id



def assign_columns(tweets_df, group_size):
    tweets_df['account_id'] = create_groups(group_size, tweets_df)
    tweets_df['tweet_id'] = range(len(tweets_df))

    tweets_df.columns =['likes', 'replies', 'retweets', 'time', 'tweet', 'year', 'month', 'day',
                         'year_month_day', 'account_id', 'tweet_id']

    column_name = ["tweet_id", "account_id", "likes",
                    "replies", "retweets", "tweet", "time", "year_month_day"]
    
    tweets_df =tweets_df[column_name]
    return tweets_df



def split_batch_and_streaming(tweets_df):
    twweets_stream = ['2017-8-25']

    tweets_batch = ['2017-8-26', '2017-8-27', '2017-8-24', '2017-8-28',
                    '2017-8-29', '2017-8-23', '2017-6-1', '2017-6-21', '2017-8-17',
                    '2017-8-18', '2017-8-14', '2017-8-22', '2017-2-19', '2017-3-28',
                    '2017-8-19', '2017-8-13', '2017-5-21', '2017-5-26', '2017-4-19',
                    '2017-8-16', '2017-8-15', '2017-6-9', '2017-8-21', '2017-6-2',
                    '2017-2-21', '2017-5-25', '2017-4-20', '2017-8-4', '2017-3-21',
                    '2017-8-20', '2017-1-11', '2017-2-10']

    tweets_stream = tweets_df.query("year_month_day in @twweets_stream")
    tweets_batch = tweets_df.query("year_month_day in @tweets_batch")



    tweets_stream.to_csv(
        "/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/stream/tweets_stream.csv", index="False")

    (tweets_batch
        .groupby("year_month_day")
        .apply(
            lambda x: x.to_csv(
                f"/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/batch/{x.name}.csv", index=False)
        )
     )

    tweets_stream.to_json(
        "/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/stream/tweets_stream.json", orient="records")

    return tweets_stream



if __name__ == "__main__":

    tweets_df = read_preprocess_tweeters(tweeter_path)

    #flattened_ids = create_groups(group_size=100, df=tweets_df)

    tweets_df = assign_columns(tweets_df, group_size=100)

    tweets_stream = split_batch_and_streaming(tweets_df)

    tweets_stream.to_json(
        "./data/preprocessed/stream/tweets_stream.json", orient="records")