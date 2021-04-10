# Imports
import pandas as pd

import requests
import yaml
import json
import os
import firebase_admin
from datetime import datetime

#Firbease (pip install firebase_admin)
from firebase_admin import credentials
from firebase_admin import firestore

from dotenv import load_dotenv
 
load_dotenv(dotenv_path='/Users/daniel/OAN/credentials/contoan/.env')

shop_url = os.environ['SHOPIFY_ACCESS_URL']

filename=os.environ['FIREBASE_FILENAME']

# Use a service account
cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()


## COGER TOTOS LOS PAYOUTS DE FIREBASE
payouts = db.collection('Accounting').where('concept','==','payout shopify').stream()

l_payouts = []
for payout in payouts:
    #print(payout)
    l_payouts.append(payout.to_dict())

print('Ultimo payout en firebase {}'.format((pd.to_datetime(pd.DataFrame(l_payouts)['execution_date'].max())
                                            ).isoformat()[0:10]
                                           )
     )
date_min = (pd.to_datetime(pd.DataFrame(l_payouts)['execution_date'].max()) + pd.DateOffset(1)).isoformat()[0:10]
date_min



r = requests.get(shop_url+"/shopify_payments/payouts.json",
                 params={
                     'limit':250,
                     'date_min':date_min,
                     "status": "paid"
                 })

payouts = r.json()

if len(payouts['payouts']) == 0:
    raise Exception('Already done with payouts')

col_query = db.collection('Collections_Info').stream()
col_info = []
for acc in col_query:
    print(u'{} => {}'.format(acc.id, acc.to_dict()))
    col_info.append(acc.to_dict())

#CREATION DATE TODAY
creation_date = datetime.today().isoformat()[:-3]+'Z'

## AÑADIRLOS A LA BBDD de FIREBASE
i=col_info[0]['code']+1
payouts2=[]
for payout in payouts['payouts'][::-1]:
    d_conta={
       "creation_date":creation_date,
       "execution_date":payout['date']+'T00:00:00.000Z',
       "place":'spain',
        #0DmODGTOEiM5lg9SGx0J - nikarit
        "project":'0DmODGTOEiM5lg9SGx0J',
        #w04441aFcU5b7pQm6Rd2 - españa
        "intervention":'w04441aFcU5b7pQm6Rd2',
        #no need for phase in movement
        "phase":None,
        # Dani - z5m936GA0t3vHM28QKhR
        # Firebase_bot - oXJJEfAEPxFYtdJ2pnaU
       "creator_user":"oXJJEfAEPxFYtdJ2pnaU",
        #Jose - IjBxXuBshlfq2MUzwBSK
        "user_in_charge":"IjBxXuBshlfq2MUzwBSK",
        "type":'movement',
        #cantidad de la transferencia
       "amount":float(payout['amount']),
        "vat":0,
        # iA9Pzv2CImjItzwCaQv0 - Banco Stripe
        "account_id":'iA9Pzv2CImjItzwCaQv0',
        # vJbbj1kPxkcdXJyBOf1l - Banco Caja Ingenieros
        "target_id":'vJbbj1kPxkcdXJyBOf1l',
       "concept":'payout shopify',
        "description":'payout de banco stripe a caja de ingenieros',
        "images":[],
        "code":i,
        "origin":'script_dani'
        
    }
   
    i=i+1
    
    doc_acc = db.collection(u'Accounting').add(d_conta)[1]
    doc_acc_id = doc_acc.id
    doc_acc.update({'id':doc_acc_id})
    
    d_conta['id'] = doc_acc_id
    payouts2.append(d_conta)
    
db.collection(u'Collections_Info').document(u'Accounting').update({'code':i})

#PRINTEAR POR PANTALLA PARA PODER MATCHEAR
print('\n'.join([' '.join([pay['id'],
                           pay['execution_date'][0:10],
                           str(pay['amount'])
                          ]) for pay in payouts2]))