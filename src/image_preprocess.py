import pandas as pd
import numpy as nd
import os
import re
import base64


def get_latitude_longitude(list_files):
    latitude = [x.split('_')[0] for x in list_files]

    name_file = [x.split('_')[1] for x in list_files]

    longitude = [x.rsplit('.', 1)[0] for x in name_file]

    return latitude, longitude


def assign_base64_column(path):
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read())

folders = os.listdir('/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/raw/image/')



def build_df(folders):
    abs_path = '/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/raw/image/'
    for folder in folders:
        path = abs_path + folder +'/'
        if folder == 'damage':
            list_file_name = os.listdir(path)
            file_path = [path + x for x in list_file_name]
            latitude, longitude =  get_latitude_longitude(list_file_name)
            encoded_string = [assign_base64_column(x) for x in file_path]
            
            data = {'damage_tag': 'damage', 'latitude': latitude, 'longitude': longitude, 'filename': list_file_name, 'file_path': file_path,'encoded_string': encoded_string}, 
            df_damage = pd.DataFrame(data)
        
        else:
            list_file_name = os.listdir(path)
            file_path = [path + x for x in list_file_name]
            latitude, longitude =  get_latitude_longitude(list_file_name)
            encoded_string = [assign_base64_column(x) for x in file_path]

            data = {'damage_tag': 'no_damage', 'latitude': latitude, 'longitude': longitude, 'filename': list_file_name, 'file_path': file_path, 'encoded_string': encoded_string}
            df_no_damage = pd.DataFrame(data)

    return pd.concat([df_damage,df_no_damage])

df = build_df(folders)
print(df)



def main():
    folders = os.listdir('/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/raw/image/')
    df = build_df(folders)

    df_json_columns= df.loc[:,['damage_tag','encoded_string','latitude','longitude']]
    df_json_columns.to_csv('/home/leshiota/Documents/Data_engineering_course/data_engineering_project/data/preprocessed/images/images_metadata.csv',index=False)

if __name__=="__main__":
    main()



