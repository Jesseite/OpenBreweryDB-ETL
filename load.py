import psycopg2
import psycopg2.extras as ex

def load_table(conn, df, schema, table): 
  
    tuples = [tuple(d) for d in df.to_numpy()] 
    col = ','.join(list(df.columns)) 

    insert = "INSERT INTO %s.%s(%s) VALUES %%s" % (schema, table, col) 
    cursor = conn.cursor() 

    try: 
        ex.execute_values(cursor, insert, tuples) 
        conn.commit() 
    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    
    print("data is inserted") 
    cursor.close() 

#postgres connection
conn = psycopg2.connect(
    host ='<your host>'
    user ='<your user>'
    password ='<your password>'
    database ='<your database>'
)

