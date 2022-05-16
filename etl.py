import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_tables import create_tables_and_schemas, drop_tables_and_schemas


def load_staging_tables(cur, conn):
    """copy data from s3 to redshift staging schema tables"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """insert data from staging schema tables into final schema tables"""
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    - creates a connection to redshift
    - drops tables and schemas
    - creates tables and schemas
    - copies data from s3 to redshift staging schema tables
    - inserts data from staging schema tables into final schema tables
    - closes the connection
    
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('connection established')
    drop_tables_and_schemas(cur, conn)
    print('tables and schemas dropped')
    create_tables_and_schemas(cur, conn)
    print('tables and schemas created')
    load_staging_tables(cur, conn)
    print('data loaded from s3 to staging')
    insert_tables(cur, conn)
    print('data transformed and inserted from staging to final dropped')
    conn.close()


if __name__ == "__main__":
    main()