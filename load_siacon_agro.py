


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
cursor.execute("""create TABLE siacon_agro(
    id integer,
    anio integer,
    id_ddr integer,
    id_cader integer,
    id_estado integer,
    id_municipio integer,
    id_ciclo integer,
    id_modalidad  integer, 
    id_cultivo integer, 
    cultivo varchar(255), 
    id_variedad varchar(255),
    nom_variedad varchar(255),
    id_unidadmedida integer,
    nom_corto varchar(255),
    id_tipoagricultura integer,
    id_tipoproduccion integer,
    id_mercado integer,
    sembrada numeric,
    cosechada numeric,
    siniestrada numeric,
    volumenproduccion numeric,
    valorproduccion numeric,
    rendimiento numeric,
    preciomediorural numeric,
    PRIMARY KEY (id),
    KEY id_estado (id_estado), 
    KEY id_municipio (id_municipio),
    CONSTRAINT Fk_siacon_inegi FOREIGN KEY (id_estado, id_municipio) REFERENCES inegi (cve_ent, cve_mun)
)
""")
db.commit()
print("Table creada exitosamente!!!")

# lectura de base en formato xlsx
base = pd.read_excel('./bases/agro_cierre_municipal.xlsx', sheet_name='SIAP')
# procesamiento
# base['CVE_ENT'] = base['CVE_ENT'].astype('int')
# base['CVE_MUN'] = base['CVE_MUN'].astype('int')
# base['CVE_LOC'] = base['CVE_LOC'].astype('int')
# base['LAT_DECIMAL'] = base['LAT_DECIMAL'].astype('float')
# base['LON_DECIMAL'] = base['LON_DECIMAL'].astype('float')
# base['POB_TOTAL'] = base['POB_TOTAL'].replace('*',np.nan).replace('-',np.nan).astype('str')
# base['POB_MASCULINA'] = base['POB_MASCULINA'].replace('*',np.nan).replace('-',np.nan).astype('str')
# base['POB_FEMENINA'] = base['POB_FEMENINA'].replace('*',np.nan).replace('-',np.nan).astype('str')
# base['TOTAL.DE.VIVIENDAS.HABITADAS'] = base['TOTAL.DE.VIVIENDAS.HABITADAS'].replace('*',np.nan).replace('-',np.nan).astype('str')
# base.head()
base = base.fillna('NA')
base.head()


# El siguiente paso consite en subir los datos a la tabla siacon_agro
#  para ello, previamente debe estar creada la tabla siacon_agro en 
#  mysql
query = """INSERT INTO siacon_agro (
    id,
    anio ,
    id_ddr ,
    id_cader ,
    id_estado,
    id_municipio ,
    id_ciclo,
    id_modalidad , 
    id_cultivo , 
    cultivo,
    id_variedad , 
    nom_variedad,
    id_unidadmedida,
    nom_corto,
    id_tipoagricultura,
    id_tipoproduccion,
    id_mercado,
    sembrada,
    cosechada,
    siniestrada,
    volumenproduccion,
    valorproduccion,
    rendimiento,
    preciomediorural) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s)"""

for i, row in base.iterrows():
    #print(row)
    #print(i)
    idx = i+1, 
    #print(f'valor i: {idx}')
    anio = row['anio'], 
    idddr = row['idddr'], 
    idcader = row['idcader'], 
    idestado = row['idestado'], 
    #nomestado = row['nomestado'], 
    idmunicipio = row['idmunicipio'],
    #nommunicipio = row['nommunicipio'], 
    idciclo = row['idciclo'], 
    #nomciclo = row['nomciclo'], 
    idmodalidad = row['idmodalidad'], 
    #nommodalidad = row['nommodalidad'],
    idcultivo = row['idcultivo'], 
    #nomcultivo = row['nomcultivo'], 
    cultivo = row['cultivo'], 
    idvariedad = row['idvariedad'], 
    #variedad = row['variedad'],
    nomvariedad = row['nomvariedad'], 
    idunidadmedida = row['idunidadmedida'], 
    #nomunidad = row['nomunidad'], 
    nomcorto = row['nomcorto'],
    idtipoagricultura = row['idtipoagricultura'], 
    #nomtipoagricultura = row['nomtipoagricultura'], 
    idtipoproduccion = row['idtipoproduccion'],
    #nomtipoproduccion = row['nomtipoproduccion'], 
    idmercado = row['idmercado'], 
    #nommercado = row['nommercado'], 
    sembrada = row['sembrada'], 
    cosechada = row['cosechada'],
    siniestrada = row['siniestrada'], 
    volumenproduccion = row['volumenproduccion'], 
    valorproduccion = row['valorproduccion'], 
    rendimiento = row['rendimiento'],
    preciomediorural = row['preciomediorural']
    #print(idx)
    #print(f'{idx}')
    values = (idx[0], anio[0] ,idddr[0],idcader[0],idestado[0],idmunicipio[0],
              idciclo[0], idmodalidad[0], idcultivo[0], cultivo[0], idvariedad[0], 
              nomvariedad[0], idunidadmedida[0], nomcorto[0], idtipoagricultura[0], 
              idtipoproduccion[0], idmercado[0], sembrada[0], cosechada[0], 
              siniestrada[0], volumenproduccion[0], valorproduccion[0], rendimiento[0],
              preciomediorural)
    
    #print(values)
    cursor.execute(query.encode("utf-8"), values)
    
    #print(idx)
cursor.close()

db.commit()

print("Carga exitosa!!!")