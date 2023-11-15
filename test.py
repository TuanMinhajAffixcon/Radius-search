import streamlit as st
from pyathena import connect
import boto3
import pandas as pd
from math import radians, acos, sin, cos
from datetime import datetime

current_date = datetime.now()
current_month = current_date.month-1
current_year=int(current_date.year)
st.set_page_config(page_title='Radius Search',page_icon='earth_asia:',layout='wide')


radius=int(st.text_input('Enter the Radius (Enter in meters)'))
file_uploader=st.sidebar.file_uploader('Upload All Location File',type=['CSV'])
if file_uploader is not None:
    location=pd.read_csv(file_uploader)


    athena_client = boto3.client('athena', region_name='ap-southeast-2')

    # # Replace 'your-access-key-id' and 'your-secret-access-key' with your AWS credentials
    conn = connect(aws_access_key_id='AKIA2ZITI36WNI35MI2Z',
                aws_secret_access_key='9Bamu2302dziWdPQ5C1SKkCQp8u7iLwiu+vKiKwG',
                s3_staging_dir='s3://tuan-query-result-bucket/query results/',
                region_name='ap-southeast-2')


    mycursor = conn.cursor()
    for i in range(len(location)):
        decimal_places = 3
        lower_lat = round((int(location['user_lat'][i] * 10**decimal_places) / 10**decimal_places)-0.001,3)
        upper_lat = round((int(location['user_lat'][i] * 10**decimal_places) / 10**decimal_places)+0.001,3)
        lower_lon = round((int(location['user_lon'][i] * 10**decimal_places) / 10**decimal_places)-0.001,3)
        upper_lon = round((int(location['user_lon'][i] * 10**decimal_places) / 10**decimal_places)+0.001,3) 



        mycursor.execute(f"select maid,latitude,longitude,year,month,datetimestamp from lifesight.tbl_movement_geohash_parquet\
                          where (month='{current_month}' and year='{current_year}')  and (latitude<={upper_lat} and latitude>={lower_lat}) and \
                          (longitude<={upper_lon} and longitude>={lower_lon}) \
                          and  cast(substring(datetimestamp,12,2) as integer) between 13 and 21")



    # results = mycursor.fetchall()
    # column_names = [desc[0] for desc in mycursor.description]
    # df_movement = pd.DataFrame(results, columns=column_names)

        chunk_size = 10000
        chunks = []

        while True:
            chunk = mycursor.fetchmany(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)

        column_names = [desc[0] for desc in mycursor.description]
        df_movement = pd.DataFrame([item for sublist in chunks for item in sublist], columns=column_names)
        # st.write(((df_movement)))

        # def calculate_distance(lat, lon):
        #     lat_rad = radians(lat)
        #     lon_rad = radians(lon)
        #     dist = 6371.01 * acos(sin(radians(location["user_lat"][0])) * sin(lat_rad) +
        #                          cos(radians(location["user_lat"][0])) * cos(lat_rad) * cos(radians(location["user_lon"][0]) - lon_rad))
        #     return dist * 1000  # Convert to meters
        
        def calculate_distance(lat, lon,user_lat,user_lon):
            lat_rad = radians(lat)
            lon_rad = radians(lon)
            dist = 6371.01 * acos(sin(radians(user_lat)) * sin(lat_rad) +
                                cos(radians(user_lat)) * cos(lat_rad) * cos(radians(user_lon) - lon_rad))
            return dist * 1000  

        df_movement['distance'] = df_movement.apply(lambda row: calculate_distance(row['latitude'], row['longitude'],location["user_lat"][i],location["user_lon"][i]), axis=1)
        df_movement=df_movement.sort_values('distance',ascending=False)
        df_movement = df_movement[df_movement['distance'] <= (radius)]
        df_movement = df_movement.drop_duplicates(subset=['maid'])

        col1,col2,col3=st.columns((3))
        with col1:
            st.write(location["user_lat"][i],location["user_lon"][i])
        with col2:
            st.write('Maid Count',len(df_movement))
        with col3:
            with st.expander("View Maid"):
                st.write(df_movement)
        
