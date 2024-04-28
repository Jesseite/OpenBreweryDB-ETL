import psycopg2
import logging
import os
from create_df_transform import fetch_normalize

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')

#Create a table with a chosen schema in PostgreSQL
def create_sql_table(c, connection):
    try: 
        c.execute("""CREATE SCHEMA IF NOT EXISTS brewdb;""")
        c.execute("""CREATE TABLE IF NOT EXISTS brewdb.brewery 
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
                  street VARCHAR(60) )""")
        # Commit the transaction
        connection.commit()
        logging.info('New table created')
    except Exception as e:
        logging.error('Failed to create table: %s', str(e))
        #Rollback the transaction in case of error
        connection.rollback()

#Insert data into the brewery table
def insert_sql_table(df, c, connection):
    #Convert the dataframe to a list of tuples 
    data = [tuple(row) for row in df.values]

    #Iterate over each row
    for row in data:
        try:
            c.execute("""INSERT INTO brewdb.brewery (
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
            # Commit the transaction
            connection.commit()
            logging.info('Data inserted')
        except Exception as e:
            #Rollback the transaction in case of error
            connection.rollback()
            logging.error('Failed to insert: %s', str(e))

def df_to_postgres_main(postgres_host, postgres_database, postgres_user, postgres_port, postgres_password):
    #Postgres connection information
    connection = psycopg2.connect(
    f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
    )
    c = connection.cursor()
    try:
        #Create the table
        create_sql_table(c, connection)
        #Call the fetch_normalize function
        df = fetch_normalize()
        #Insert the fetched data into the table
        insert_sql_table(df, c, connection)
    except Exception as e:
        logging.error('Failed to execute: %s', str(e))
    finally:
        c.close()
        connection.close()

df_to_postgres_main(postgres_host, postgres_database, postgres_user, postgres_port, postgres_password)