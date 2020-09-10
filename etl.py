import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Queries which load data from S3 buckets
    to Redshift
    INPUTS:
    cur = cursor variable of the database
    conn = connection variable of the database
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error loading staging tables" + query)
            print(e)
            
    print('Tables Staged Sucessfully')

def insert_tables(cur, conn):
    """
    INSERT data using queries from staging tables to 
    the dimension and fact tables
    INPUTS:
    cur = cursor variable of the database
    conn = connection variable of the database
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error Inserting tables" + query)
            print(e)
            
    print('Tables Inserted Sucessfully')

def main():
    """
    Creates the connection to the database using credential in the dwh config file
    then executes the load_staging_tables and insert_tables query
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()