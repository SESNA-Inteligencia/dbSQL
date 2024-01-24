
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
import math
############################################################################
#                        Configuración para Conexión 
############################################################################
user='root'
password='astro123'
host='localhost'
database='psociales'

# coneccion con MySQL
db= mysql.connector.connect(user=user, 
                            password=password,
                            host=host,
                            database=database)
#cnx.close()
cursor = db.cursor()

##############################################################################
#                       Creación de tabla en MySQl
##############################################################################
# Si existe previamente la tabla centros_acopio la elimina
cursor.execute('DROP TABLE IF EXISTS centros_acopio')
# Genera la tabla centros_acopio
cursor.execute("""create TABLE centros_acopio(
    id INT NOT NULL,
    year int, 
    cve_ent int,
    cve_mun int,
    cve_loc	int,
    latitud float,
    longitud float,	
    PRIMARY KEY (id),
    KEY (cve_ent),
    KEY (cve_mun),
    KEY (cve_loc)
)
""")

db.commit()
print("Table creada exitosamente!!!")
##############################################################################
#                       Subida a Base de Datos
##############################################################################
# lectura de base en formato xlsx
base = pd.read_excel('./bases/CentrosAcopio.xlsx')
# Procesamiento
base['id'] = [i for i in range(1,len(base['year'])+1)]
base['year'] = base['year'].astype('int')
base['cve_ent'] = base['cve_ent'].fillna(np.nan)
base['cve_mun'] = base['cve_mun'].fillna(np.nan)
base['cve_loc'] = base['cve_loc'].fillna(np.nan)
base['latitud'] = base['latitud'].astype('float')
base['longitud'] = base['longitud'].astype('float')
base.head()

# El siguiente paso consite en subir los datos a la tabla siacon_agro
#  para ello, previamente debe estar creada la tabla siacon_agro en 
#  mysql
query = """INSERT INTO centros_acopio (id, year, cve_ent, cve_mun, cve_loc,
    latitud, longitud) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)"""

for i, row in base.iterrows():
    #print(row)
    id = row['id']
    date = row['year']
    cve_ent = np.where(math.isnan(row['cve_ent']),None, row['cve_ent']).tolist()
    cve_mun = np.where(math.isnan(row['cve_mun']),None, row['cve_mun']).tolist() 
    cve_loc = np.where(math.isnan(row['cve_loc']),None, row['cve_loc']).tolist()
    lat = np.where(math.isnan(row['latitud']),None, row['latitud']).tolist()
    lon = np.where(math.isnan(row['longitud']),None, row['longitud']).tolist()
    #print(f'{idx[0]}')
    
    values = (id,date,cve_ent,cve_mun,cve_loc,lat,lon)
    #print(values)
    cursor.execute(query.encode("utf-8"), values)
    #print(idx)
cursor.close()

db.commit()

###############################
print("Carga exitosa!!!")