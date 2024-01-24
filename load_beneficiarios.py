

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

# Configuración para conexión
user='root'
password='astro123'
host='localhost'
database='psociales'

# coneccion con mysql 
db= mysql.connector.connect(user=user, 
                            password=password,
                            host=host,
                            database=database)
#cnx.close()
cursor = db.cursor()

# Si existe previamente la tabla Beneficiarios la elimina
cursor.execute('DROP TABLE IF EXISTS Beneficiarios')
# Generación de la tabla Beneficiarios
cursor.execute("""create TABLE Beneficiarios(
    id varchar(20) NOT NULL,
    year int,
    cve_ent int,
    cve_mun int,
    cve_loc int,
    cultivo varchar(20) DEFAULT NULL,
    idcultivo int DEFAULT NULL,
    nomciclo varchar(60) DEFAULT NULL,
    pgarantia float DEFAULT NULL,
    preferencia float DEFAULT NULL,
    volincentivado float DEFAULT NULL,
    monto_total float DEFAULT NULL,
    tipo varchar(20) DEFAULT NULL,
    PRIMARY KEY (id),
    KEY cve_ent (cve_ent), 
    KEY cve_mun (cve_mun),
    KEY cve_loc (cve_loc)
)
""")
db.commit()
print("Table creada exitosamente!!!")
##############################################################################
#                       Subida a Base de Datos
##############################################################################
# lectura de base en formato xlsx
base = pd.read_csv('./bases/PBeneficiarios.csv', low_memory=False)

base['cve_ent'] = [np.nan if val == 'NA' else val for val in base['cve_ent']]
base['cve_mun'] = [np.nan if val == 'NA' else val for val in base['cve_mun']]
base['cve_loc'] = [np.nan if val == 'NA' else val for val in base['cve_loc']]

#base = base.fillna(value=np.nan)
nulls = np.nan
# Procesamiento 
base['id'] = base['id'].astype('str')
base['year'] = base['year'].astype('int')
base['cve_ent'] = base['cve_ent'].fillna(np.nan)
base['cve_mun'] = base['cve_mun'].fillna(np.nan)
base['cve_loc'] = base['cve_loc'].fillna(np.nan)
base['cultivo'] = base['cultivo'].astype('str').fillna(value=nulls)
base['idcultivo'] = base['idcultivo'].fillna(value=nulls)
base['nomciclo'] = base['nomciclo'].astype('str').fillna(value=nulls)
base['pgarantia'] = base['pgarantia'].fillna(value=nulls)
base['preferencia'] = base['preferencia'].fillna(value=nulls)
base['volincentivado'] = base['volincentivado'].fillna(value=nulls)
base['monto_total'] = base['monto_total'].fillna(value=nulls)
base['tipo'] = base['tipo'].astype('str').fillna(value=nulls)
base.head()
print("Table creada exitosamente!!!")
# El siguiente paso consite en subir los datos a la tabla siacon_agro
#  para ello, previamente debe estar creada la tabla siacon_agro en 
#  mysql

#id, year,cve_ent, cve_mun, cve_loc, cultivo,idcultivo, nomciclo, pgarantia, preferencia, vincentivado, monto_total, tipo
query = """INSERT INTO Beneficiarios (id, year, cve_ent, cve_mun, cve_loc, cultivo, idcultivo, nomciclo, pgarantia, preferencia, volincentivado, monto_total, tipo) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

for i, row in base.iterrows():
    #print(row)
    id = row['id']
    year = row['year']
    cve_ent = np.where(math.isnan(row['cve_ent']),None, row['cve_ent']).tolist()
    cve_mun = np.where(math.isnan(row['cve_mun']),None, row['cve_mun']).tolist() 
    cve_loc = np.where(math.isnan(row['cve_loc']),None, row['cve_loc']).tolist()
    cultivo =  row['cultivo']
    idcultivo = np.where(math.isnan(row['idcultivo']),None, row['idcultivo']).tolist()#row['idcultivo']
    nomciclo = row['nomciclo']
    pgarantia = np.where(math.isnan(row['pgarantia']),None, row['pgarantia']).tolist() #row['pgarantia']
    preferencia = np.where(math.isnan(row['preferencia']),None, row['preferencia']).tolist() #row['preferencia']
    volincentivado = np.where(math.isnan(row['volincentivado']),None, row['volincentivado']).tolist() #row['volincentivado']
    monto_total = np.where(math.isnan(row['monto_total']),None, row['monto_total']).tolist()#row['monto_total']
    tipo = row['tipo']
    
    #print(f'{idx[0]}')
    values = (id, year, cve_ent, cve_mun, cve_loc,
              cultivo, idcultivo, nomciclo, pgarantia,
              preferencia, volincentivado, monto_total, tipo)
    #print(values)
    cursor.execute(query.encode("utf-8"), values)
    #print(idx)
cursor.close()

db.commit()

###############################
print("Carga exitosa!!!")