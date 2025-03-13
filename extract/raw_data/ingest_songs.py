import json
import pandas as pd
import os
import pandas_gbq
from dotenv import load_dotenv

# Load Environments Variables
load_dotenv()

RAW_SONGS_TABLE= os.environ['RAW_SONGS_TABLE_ID']
PROJECT_ID= os.environ['PROJECT_ID']

with open('extract/raw_data/songs_data.json', 'r') as file:
    data = json.load(file)

# Create a bigquery table with songs library
df = pd.DataFrame(data)
pandas_gbq.to_gbq(df, RAW_SONGS_TABLE, PROJECT_ID, if_exists='append')

print(f'✅ Songs data pushed to bigquery table {RAW_SONGS_TABLE}')
