import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data from S3 storage into staging tables which will be used for later processing.
    :param cur: cursor needed to connect and process data from Redshift tables
    :param conn: connection parameter to Redshift cluster, included in dwh.cfg file
    :return: execute query directly in Redshift cluster
    """
    for query in copy_table_queries:
        print('staging', query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function uses staging table as base and inputs into target tables appropriate data.
    :param cur: cursor needed to connect and process data from Redshift tables
    :param conn: connection parameter to Redshift cluster, included in dwh.cfg file
    :return: execute query directly in Redshift cluster
    """
    for query in insert_table_queries:
        print('insert', query)
        cur.execute(query)
        conn.commit()


def main():
    """
    This is orchestrator for loading into staging tables data from S3 storag and then parse this data to correct target tables on Redshift cluster.
    :return: staging tables are fed with data and star schema is populated with correct values
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