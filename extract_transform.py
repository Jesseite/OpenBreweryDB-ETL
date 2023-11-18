import pandas as pd
import requests
import json
import psycopg2
from load import load_table

CITY = input('Pick a city: ')
NUM = input('How many breweries do you want to see?: ')

#fetch data from Open Brewery DB
fetch = f"https://api.openbrewerydb.org/v1/breweries/?by_city={CITY}&per_page={NUM}"
r = requests.get(fetch)
json_obj = r.json()

#prettify print json data
prettify = json.dumps(json_obj, indent=2)
print(prettify)

#flatten json
df = pd.json_normalize(json_obj)

#dictionary mapping column names to datatypes
column_type = {
    'id': 'string',
    'name': 'string',
    'brewery_type': 'string',
    'address_1': 'string',
    'address_2': 'string',
    'address_3': 'string',
    'city': 'string',
    'state_province': 'string',
    'postal_code': 'string',
    'country': 'string',
    'longitude': 'float',
    'latitude': 'float',
    'phone': 'string',
    'website_url': 'string',
    'state': 'string',
    'street': 'string'
}

#transform json objects to appropriate datatypes
for json_data, column_type in column_type.items():
    if json_data in df.columns:
        df[json_data] = df[json_data].astype(column_type)

print(df.info())

#replace 'NAType' values with None
df = df.replace({pd.NA: None})

#postgres connection
conn = psycopg2.connect(
    database='postgres',
    user='postgres',
    password='password',
    host='localhost'
)

#import function from the python file - load.py
load_table(conn, df, 'open_brewery_db', 'brewery')
