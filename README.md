# Open Brewery DB ETL

## Information

![image](https://github.com/Jesseite/OpenBreweryDB-ETL-Python/blob/main/images/Diagram.png)

## Open Brewery DB
Open Brewery DB is a free and open-source dataset and API that provides public information on breweries.
https://www.openbrewerydb.org/

## Python
### Steps
'create_df_transform' -> Fetch data from the Open Brewery DB API. Flatten JSON and transform it into a dataframe so that it can be easily loaded into a SQL table.

'df_to_postgres' -> Load the dataframe into a PostgreSQL table.

## PostgreSQL
This is a local PostgreSQL server. I recommend downloading PgAdmin whcih is a GUI for PostgreSQL.