import psycopg2
import time

def load_data(filepath, table_name, db_params):
    try:
        conn = psycopg2.connect(**db_params)
        conn.autocommit = False
        cursor = conn.cursor()
        
        with open(filepath, 'r') as file:
            next(file)
            start_time = time.time() 
            cursor.copy_from(file, table_name, sep=',', null='')
            elapsed_time = time.time() - start_time
            print(f"data loaded in {elapsed_time:.2f} seconds.")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"error: {e}")
    finally:
        cursor.close()
        conn.close()
db_params = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "postgres",
    "password": "REDACTED"
}
load_data('acs2015_census_tract_data_part2.csv', 'censusdata', db_params)
