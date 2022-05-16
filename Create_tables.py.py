import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables_and_schemas(cur, conn):
    """drop all tables and schemas"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables_and_schemas(cur, conn):
    """creates all tables and schemas"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """drops and creates tables and schemas"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn.set_session()
    cur = conn.cursor()
    drop_tables_and_schemas(cur, conn)
    create_tables_and_schemas(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()