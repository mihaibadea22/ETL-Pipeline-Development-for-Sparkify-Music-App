# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 16:21:31 2024

@author: Mihai
"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def terminate_active_sessions(cur, conn, dbname):
    """
    Terminates all active connections to the specified database.
    """
    terminate_query = f"""
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '{dbname}' AND pid <> pg_backend_pid();
    """
    cur.execute(terminate_query)
    conn.commit()

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb

    """
    
    # connect to default database
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=root123")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    
    # close connection to default database
    conn.close()
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=root123")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in 'drop_table_queries' list
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    
    """
    Creates each table using the queries in 'create_table_queries' list
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def main():
    
    """
    - drop if exists and creates the sparkify db
    - establish connection with the sparkify db and gets cursor to it
    - drops all tables
    - creates all tables needed
    - finally close the connection
    
    """
    cur, conn = create_database()
    
    drop_tables(cur,conn)
    create_tables(cur,conn)
    
    conn.close()


if __name__ == "__main__":
    main()                     
    
    
    