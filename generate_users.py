import requests
import json
import datetime
import pandas as pd
import duckdb
import time

URL='https://randomuser.me/api/'
NUM_OF_LOOP=100


def get_user_data(url):
    request = requests.get(url)
    data = request.json()
    return data['results']

all_data = []

for i in range(1,NUM_OF_LOOP+1):
    data = get_user_data(URL)
    all_data.extend(data)
    time.sleep(0.5)
    
with open('data/users_data.json', 'w') as file:
    json.dump(all_data, file, indent= 4)
    
print('✅ Data saved into .json file')
df = pd.json_normalize(all_data, max_level= 1)

df.to_csv('data/users_data.csv', index= False)

print('✅ User data saved into .csv file')
print('✅ Script ended')
