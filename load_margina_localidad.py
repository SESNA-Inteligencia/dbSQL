

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

# Generación de tabla
cursor.execute("""create TABLE conapo_IMM(
    id INT NOT NULL PRIMARY KEY,
    anio INT,
    cve_ent INT,
    cve_mun INT,
    cve_loc INT,
    POB_ INT,
    ANALF FLOAT,
    SBASC FLOAT,
    OVSDE FLOAT,
    OVSEE FLOAT,
    OVSAE FLOAT,
    OVPT FLOAT,
    OVHAC FLOAT,
    IM_2020 FLOAT,
    GM_2020 VARCHAR(20),
    IMN_2020 FLOAT, 
    KEY CVE_ENT (CVE_ENT), 
    KEY CVE_MUN (CVE_MUN),
    KEY CVE_LOC (CVE_LOC)
)
""")
db.commit()
print("Table creada exitosamente!!!")

# lectura de base en formato xlsx
base = pd.read_excel('./bases/IML_2020.xlsx', sheet_name='IM')
# procesamiento
base['ID'] = [int(i+1) for i in range(len(base['ENT']))]
base['ANIO'] = [2020 for i in range(len(base['ENT']))]
base['ENT'] = base['ENT'].astype('int')
base['MUN'] = base['MUN'].astype('int')
base['LOC'] = base['LOC'].astype('int')
base['POB_TOT'] = base['POB_TOT'].astype('int')

base['ANALF'] = base['ANALF'].astype('float')
base['SBASC'] = base['SBASC'].astype('float')
base['OVSDE'] = base['OVSDE'].astype('float')
base['OVSEE'] = base['OVSEE'] .astype('float')
base['OVSAE'] = base['OVSAE'].astype('float')
base['OVPT'] = base['OVPT'].astype('float')
base['OVHAC'] = base['OVHAC'].astype('float')
#base['OVSREF'] = base['OVSREF'].astype('float')
# base['PL5000'] = base['PL.5000'].astype('str')
# base['PO2SM'] = base['PO2SM'].astype('str')
base['IM_2020'] = base['IM_2020'].astype('float')
base['GM_2020'] = base['GM_2020'].astype('str')
base['IMN_2020'] = base['IMN_2020'].astype('float')
#base = base.fillna('NA')
print('Procesamiento exitoso!!')

# El siguiente paso consite en subir los datos a la tabla siacon_agro
#  para ello, previamente debe estar creada la tabla siacon_agro en 
#  mysql
query = """INSERT INTO coneval_margina_localidad(ID,ANIO,CVE_ENT,CVE_MUN,CVE_LOC,
    POB_TOT,ANALF,SBASC,OVSDE,OVSEE,OVSAE,OVPT,OVHAC,IM_2020,GM_2020,
    IMN_2020) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

for i, row in base.iterrows():
    #print(row)
    # #print(i)
    id = row['ID'],
    anio = row['ANIO'],
    ent = row['ENT'],
    mun = row['MUN'],
    loc = row['LOC'],
    pob_total = row['POB_TOT'],
    analf = row['ANALF'],
    sbasc = row['SBASC'],
    ovsde = row['OVSDE'],
    ovsee = row['OVSEE'],
    ovesae = row['OVSAE'],
    ovpt = row['OVPT'],
    ovhac = row['OVHAC'],
    #ovsref = row['OVSREF'],
    # pl = row['PL5000'],
    # po2sm = row['PO2SM'],
    im = row['IM_2020'],
    gm = row['GM_2020'],
    imn = row['IMN_2020']
    
    #print(idx)
    #print(f'{idx}')
    values = (id[0],anio[0],ent[0],mun[0],loc[0],pob_total[0],analf[0],sbasc[0],
              ovsde[0],ovsee[0],ovesae[0],ovpt[0],ovhac[0],
              im[0],gm[0],imn)
    
    #print(values)
    cursor.execute(query.encode("utf-8"), values)
    
    #print(idx)
cursor.close()

db.commit()

print("Carga exitosa!!!")