import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This function connects to Redshift cluster and drops all tables if they exist to ensure clear start.
    :param cur: cursor needed to connect and process data from Redshift tables
    :param conn: connection parameter to Redshift cluster, included in dwh.cfg file
    :return: execute query directly in Redshift cluster
    """
    for query in drop_table_queries:
        print('drop', query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function connects to Redshift cluster and creates new tables for further processing. We will use staging tables fed from S3 repository as interim state before populating targed star schema tables.
    :param cur: cursor needed to connect and process data from Redshift tables
    :param conn: connection parameter to Redshift cluster, included in dwh.cfg file
    :return: execute query directly in Redshift cluster
    """
    for query in create_table_queries:
        print('create', query)
        cur.execute(query)
        conn.commit()


def main():
    """
    This is main orchestrator for clearing Redshift cluster of remaining tables (drop_tables) and creating new schema (create_tables).
    :return: new tables star schema is ready to be fed with data using interim staging tables with pre-processed data
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()