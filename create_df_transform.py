import requests
import json
import pandas as pd

#Fetch data from Open Brewery DB
def fetch_normalize():
    fetch = f"https://api.openbrewerydb.org/v1/breweries/?per_page=5"
    response = requests.get(fetch)
    json_object = response.json()

    #Check if the status code is correct
    if response.status_code == 200:
        #Make json data look pretty
        prettify = json.dumps(json_object, indent=2)
        print('Here is the information')
        print(prettify)
    else:
        print(f'error {response.status_code}: Failed fetching brewery data')

    #Flatten json data
    df = pd.json_normalize(json_object)
    
    #dictionary mapping column names to datatypes
    column_type = {
        'id': 'object',
        'name': 'object',
        'brewery_type': 'object',
        'address_1': 'object',
        'address_2': 'object',
        'address_3': 'object',
        'city': 'object',
        'state_province': 'object',
        'postal_code': 'object',
        'country': 'object',
        'longitude': 'float',
        'latitude': 'float',
        'phone': 'object',
        'website_url': 'object',
        'state': 'object',
        'street': 'object'
    }

    #transform json objects to appropriate datatypes
    for json_object, column_type in column_type.items():
        if json_object in df.columns:
            df[json_object] = df[json_object].astype(column_type)

    print(df.info())

    #replace 'NAType' values with None
    df = df.replace({None: None})
    return df

fetch_normalize()












