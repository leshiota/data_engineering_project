import pandas as pd
import random

def read_csv(path):

    df_image = pd.read_csv(path)
    return df_image

def read_json(path):
    json_tweets = pd.read_json(path)
    return json_tweets

def create_id(json_tweets,df_image):
    tweets_id = json_tweets['tweet_id'].to_list()
    random.seed(26)
    random_id = random.sample(tweets_id, len(df_image))
    df_image['tweet_id'] = random_id    

    return df_image

def merge_tweets_image(json_tweets,df_image):
    merge_tweets_image = json_tweets.merge(df_image, on='tweet_id',how='left')
    new_name_columns = ['tweet_id', 'account_id', 'likes', 'replies', 'retweets', 'tweet',
                        'time', 'year_month_day', 'damage_flag', 'image_base64', 'latitude',
                        'longitude']
    
    merge_tweets_image.columns =new_name_columns
    return merge_tweets_image

def save_json(df,path,orient):
    df.to_json(path,orient)

def main():
    path_image = '/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/images/images_metadata.csv'
    path_tweets = '/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/stream/tweets_stream.json'

    df_image = read_csv(path_image)
    json_tweets = read_json(path_tweets)
    df_image = create_id(json_tweets,df_image)
    df_merge= merge_tweets_image(json_tweets,df_image)
    save_json(df_merge, "/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/merge/merge_tweets_image_strem.json", orient="records")

    df_sample = df_merge.iloc[0:100, :]
    save_json(df_sample, "/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/sample/sample_merge_tweets_image_strem.json", orient="records")

if __name__=="__main__":
    main()
