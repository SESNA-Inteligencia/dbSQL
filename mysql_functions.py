import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine
import sys
import pymysql
import mysql
import mysql.connector
from mysql.connector import Error
import math 
import os


# crea conexión
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Conexión exitosa")
    except Error as e:
        print(f"Error ocurrido '{e}' ")

    return connection

# Función para ejecutar consultas en MySQL (query)
def execute_query(connection, query, values=None):
    cursor = connection.cursor()
    try:
        if values:
            cursor.executemany(query, values)
        else:
            cursor.execute(query)
        connection.commit()
        print("Query ejecutada exitosamente")
    except Error as e:
        print(f"Error ocurrido '{e}'")


# Inserta datos a MySQL
def insert_data_to_mysql(connection, table_name, df):
    
    # genera %s para integrar numero de columnas 
    ncols = len(df.columns)
    result = ["%s"] * ncols
    output = ", ".join(result)
    
    try:
        cursor = connection.cursor()
        for index, row in df.iterrows():
            values = tuple(row)
            # query
            insert_query = f"INSERT INTO {table_name} VALUES ({output})"
            cursor.execute(insert_query, values)
        connection.commit()
        cursor.close()
    except Error as e:
        print("Error:", e)