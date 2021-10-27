#to open cred files
import os
import yaml
#cleaning and operations
import pandas as pd
import numpy as np
#to read translations
import json
import unidecode
import requests

#bigquery
import pandas_gbq
from google.cloud import bigquery
## got to service accounts in google cloud in project OAN-Nikarit and download json file
from dotenv import load_dotenv
load_dotenv('/Users/daniel/OAN/credentials/contoan/.env')

#FIREBASE
import firebase_admin
#Firbease (pip install firebase_admin)
from firebase_admin import credentials
from firebase_admin import firestore

filename=os.environ['FIREBASE_FILENAME']

#OPEN CONECTION TO FIREBASE
cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()

col_query = db.collection('Nikarit_Sales').stream()

ventas = []
for acc in col_query:
    ventas.append(acc.to_dict())
    
df_ventas = pd.DataFrame(ventas)
cols = list(df_ventas.columns)
cambios = [col for col in cols if 'price' in col]
for cambio in cambios:
    df_ventas[cambio]=pd.to_numeric(df_ventas[cambio])
    
df_ventas['creation_date']=pd.to_datetime(df_ventas['creation_date'],utc=True)
df_ventas['close_date']=pd.to_datetime(df_ventas['close_date'],utc=True)

#lista de mujeres
url = 'https://raw.githubusercontent.com/marcboquet/spanish-names/master/mujeres.csv'
df_mujeres = pd.read_csv(url, error_bad_lines=False)
l_mujeres = df_mujeres['nombre'].to_list()

l_mujeres = [str(nom).lower() for nom in l_mujeres]

l_mujeres= l_mujeres+['maasuncion','mirenchu','begona','nadai','sra.','mcarmen','carmena','maita',
'madolores','reyero','ma','maconcepción','cris']

#lista de hombres
url = 'https://raw.githubusercontent.com/marcboquet/spanish-names/master/hombres.csv'
df_hombres = pd.read_csv(url, error_bad_lines=False)
l_hombres = df_hombres['nombre'].to_list()

l_hombres = [nom.lower() for nom in l_hombres]
l_hombres= l_hombres+['fcojavier']

df_ventas['client_name'].value_counts()

#aplicar sexo
df_ventas['client_gender'] = df_ventas.apply(lambda x: 
                            'F' if unidecode.unidecode(
                                    str(x['client_name']).replace(" ", "").lower()) in l_mujeres
                            else 'M' if unidecode.unidecode(
                                str(x['client_name']).replace(" ", "").lower())  in l_hombres
                            else 'F' if unidecode.unidecode(
                                str(x['client_name']).split(" ")[0].lower())  in l_mujeres
                            else 'M' if unidecode.unidecode(
                                str(x['client_name']).split(" ")[0].lower())  in l_hombres

                            else 'N/A' , axis =1)


## METER DONACIONES
def don(x):
    total_price = x['total_price']
    subtotal_price = x['subtotal_price']
    fecha = str(x['creation_date']).split(' ')[0]
    origin = x['origin']
    don = 0.0
    if '2020-11-01' < fecha and origin == "Shopify":
        if total_price < 40:
            don = total_price - subtotal_price - 5.50
        else:
            don = total_price - subtotal_price
            
    return don

df_ventas['donation'] = df_ventas.apply(lambda x: don(x),axis=1)

## METER PROVINCIA
shop_url = os.environ['SHOPIFY_ACCESS_URL']
dates = ['2019-01-01T00:15:47-04:00','2020-01-01T00:15:47-04:00',
         '2020-05-01T00:15:47-04:00','2020-12-10T00:15:47-04:00', '2021-01-10T00:15:47-04:00']
l_orders = []

for date_min,date_max in zip(dates[:-1],dates[1:]):
    orders_url = shop_url+'/orders.json'
    r = requests.get(orders_url,
                 params={
                        'limit':250,
                        'status':'any',
                         'created_at_max':date_max,
                         'created_at_min':date_min
                        })
    orders = pd.DataFrame(r.json()['orders'])
    l_orders.append(orders)
df_orders = pd.concat(l_orders,axis=0).reset_index().drop_duplicates(subset=['id'])
df_orders['province_2'] = df_orders['billing_address'].apply(lambda x: x['province'])
df_orders['order_id'] = df_orders['id'].apply(str)
df_ventas['order_id'] = df_ventas['order_id'].apply(str)
df_ventas = pd.merge(df_ventas,df_orders[['order_id','province_2']],on='order_id',how='left')

df_ventas['province'] = df_ventas['province_2'] 
df_ventas = df_ventas.drop('province_2',axis=1)

##QUITAR COLUMNAS QUE NO NOS INTERESAN
cols = list(df_ventas.columns)
cols.pop(cols.index('to'))
cols.pop(cols.index('message'))
cols.pop(cols.index('navidadCode'))

df_ventas = df_ventas[cols]

#METER IVA BIEN
def iva(x):
    iva = x['total_tax']
    
    #Corregir ventas de contoan
    if x['origin'] == 'Contoan':
        iva = x['total_price']*(1-1/1.21)
    return iva

df_ventas["total_tax_2"] = df_ventas.apply(lambda x: iva(x),axis=1)
df_ventas["total_tax"] = df_ventas["total_tax_2"]
df_ventas = df_ventas.drop('total_tax_2',axis=1)

##METER SUBTOTAL BIEN
def subtotal(x):
    subtotal = x['subtotal_price']
    
    #Corregir ventas de contoan
    if x['origin'] == 'Contoan':
        subtotal = x['total_price']
    return subtotal

df_ventas["subtotal_price_2"] = df_ventas.apply(lambda x: subtotal(x),axis=1)
df_ventas["subtotal_price"] = df_ventas["subtotal_price_2"]
df_ventas = df_ventas.drop('subtotal_price_2',axis=1)


bgq_client = bigquery.Client()
dataset_id = "{}.contoan".format(bgq_client.project)
dataset = bigquery.Dataset(dataset_id)

cols = list(df_ventas.columns)
cols.sort()

#subir a bigquery
table_id = "contoan.sales"

df_ventas[cols].to_gbq(table_id, project_id=bgq_client.project, if_exists='replace')

##MODIFICAR BOOL VARIABLES
bgq_client.query("""
CREATE OR REPLACE TABLE `oan-nikarit.contoan.sales` AS
SELECT
  * EXCEPT (buyer_accepts_marketing,taxes_included),
  CAST(buyer_accepts_marketing AS BOOL) AS buyer_accepts_marketing,
  CAST(taxes_included AS BOOL) AS taxes_included
FROM
  `oan-nikarit.contoan.sales`
""")

print('Done')