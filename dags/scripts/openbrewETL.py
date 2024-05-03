#!/usr/bin/env python
import pandas as pd
import requests
import json
import psycopg2

def extractTransform():
    #Extract data from Open Brewery DB
    fetch = f"https://api.openbrewerydb.org/v1/breweries/random"
    response = requests.get(fetch)
    json_object = response.json()

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
    df = df.replace({pd.NA: None})
    return df

#Loading stage - load data into PostgreSQL table
def createTable(conn):
    cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS public.brewery 
        (id VARCHAR(60) PRIMARY KEY,
        name VARCHAR(120),
        brewery_type VARCHAR(60),
        address_1 VARCHAR(60),
        address_2 VARCHAR(60),
        address_3 VARCHAR(60),
        city VARCHAR(60),
        state_province VARCHAR(60),
        postal_code CHAR(10),
        country VARCHAR(60),
        longitude NUMERIC(120,0),
        latitude NUMERIC(120,0),
        phone VARCHAR(60),
        website_url VARCHAR(60),
        state VARCHAR(60),
        street VARCHAR(60));
        """)
    except (Exception, psycopg2.DatabaseError) as e: 
        print(e) 
        conn.rollback()
    else:
        conn.commit()

def insertTable(df, conn):
    #Convert the dataframe to a list of tuples 
    data = [tuple(row) for row in df.values]
    #Iterate over each row
    cur = conn.cursor()
    for row in data:
        try:
            cur.execute("""INSERT INTO public.brewery (
                    id,
                    name,
                    brewery_type,
                    address_1,
                    address_2,
                    address_3,
                    city,
                    state_province,
                    postal_code,
                    country,
                    longitude,
                    latitude,
                    phone,
                    website_url,
                    state,
                    street)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", row)
            conn.commit() 
        except (Exception, psycopg2.DatabaseError) as e: 
            print("Error: %s" % e) 
            conn.rollback() 
            cur.close() 
            return 1
    cur.close()

def main():

    conn = psycopg2.connect(
        host="postgres", # changed from 'localhost' so it would work with docker
        database="dbspace",
        user="postgres",
        password="password")
    
    data = extractTransform()
    createTable(conn)
    insertTable(data, conn)
    print('Data inserted')

    conn.close()

if __name__ == "__main__":
    main()

    
