import csv
import json
import pandas as pd

with open('data/songs_data.json', 'r') as file:
    data = json.load(file)
    

df = pd.DataFrame(data).to_csv('data/songs_data.csv', index= False)

print('✅ Songs data saved into .csv file')
