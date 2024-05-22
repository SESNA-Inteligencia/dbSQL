
##############################################################################
#                       Creación de tabla en MySQl
##############################################################################

# !pip install mysql.connector
# !pip install requests
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
# importa funciones para crear , ejecutar e insertar datos a MySQL
from mysql_functions import create_connection, execute_query, insert_data_to_mysql


########################################################################
# Configuración para conexión
########################################################################
host_name='localhost'
user_name='root'
user_password='astro123'
db_name='psociales'

# Create conección a base de datos MySQL 
connection = create_connection(host_name, user_name, user_password, db_name)


# Elimina tabla si existe previamente
query1 = "DROP TABLE IF EXISTS delitos"

# Consulta para crear tabla en la base 'db_name' previamente creada
query2 = """create TABLE delitos(
    date varchar(120),	
    anio int,	
    mes	varchar(120),
    mes_num	int,
    cve_ent	varchar(60),
    nom_ent	varchar(120),
    cve_mun	varchar(60),
    nom_mun	varchar(120),
    tipo_delito	varchar(220),
    subtipo_delito varchar(240),	
    modalidad varchar(240), 
    cantidad int,	
    latitud	float,
    longitud float,	
    pob_total varchar(120),	
    pob_masculino varchar(120),	
    pob_femenina varchar(120),  
    KEY cve_ent (cve_ent), 
    KEY cve_mun (cve_mun)
)
""" 
# Ejecuta la cunsultas
#  Si se requiere alguna otra consulta adicional, se genera queryn y se ejecuta 
if connection:
    execute_query(connection, query1)
    execute_query(connection, query2)

#######################################################################
#   Carga de datos un archivo por estado
#######################################################################
# el orden de los campos del dataframe debe coincidir con el orden en el que se creó la tabla 
#  en caso de no ser así, deberán ordenarse previamente

# Lectura de datos
df = pd.read_csv('./datasets/base_delitos_cleaned.csv', converters={'cve_ent':str, 'cve_mun':str})

print(f'columns {df.columns}')
df2 = df.dropna()

# cleaning:
#  Espacio para limpieza n caso de ser necesaria

#df2['cve_ent'] = df2['cve_ent'].astype('str')
#df2['cve_mun'] = df2['cve_ent'].astype('str')
#print(df2.columns)

# función para subir datos a MySQL
#  el nombre de la tabla debe coincidir con el nombre de la tabla previamente creada
insert_data_to_mysql(connection, table_name="delitos", df=df2)

# Cierra conexión 
if connection:
    connection.close()        
        
print('---- Carga exitosa ----')    




