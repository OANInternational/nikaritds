'''
Este script coge los datos del los payouts de shopify y registra los payouts en Firebase e inserta el id en Diario2022

Aquellos payouts que no hayan sido contabilizados en Firebase y en el excel, los registra. No supera la ultima fecha de Diario2020.
Registra primero el movimiento en Firebase y luego inserta el id en la columna de matchId.

Ademas actualiza el saldo de Caja de Ingenieros y Stripe trans registrar todos los movimientos y el counter.

MEJORAS A IMPLEMENTAR:
La columna en donde se encuentra el MATCH ID estan hardcodeadas. Debería detectarse por si algun usuario inserta columnas en las hojas.
'''

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

### google sheet api
from oauth2client import file,client, tools
from googleapiclient import discovery
from httplib2 import Http

 
load_dotenv(dotenv_path='/Users/daniel/OAN/credentials/contoan/.env')

shop_url = os.environ['SHOPIFY_ACCESS_URL']

filename=os.environ['FIREBASE_MIONG_FILENAME']

OAN_account = "5Tv2u4n8BReebmKUNIuN"

CAJAINGENIEROS = "vJbbj1kPxkcdXJyBOf1l"
BANCOSTRIPE = "iA9Pzv2CImjItzwCaQv0"
GENERAL2022NIKARIT = "WYTJNewInibo0kumno7l"
NIKARIT = "0DmODGTOEiM5lg9SGx0J"
FIREBASEACCOUNT = "oXJJEfAEPxFYtdJ2pnaU"
COLUMNAMATCHID = "G"


##connect to gsheets
## CONECTION TO DRIVE

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets'
        ]
store = file.Storage('/Users/daniel/OAN/credentials/storage.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('/Users/daniel/OAN/credentials/credentials.json', SCOPES)
    flags= tools.argparser.parse_args(args=[])
    creds = tools.run_flow(flow, store, flags=flags)

sheets_service = discovery.build('sheets', 'v4', http=creds.authorize(Http()))
SPREADSHEET_ID = os.environ['DIARIO_2022']

def update_values(range_name,values):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.\n"
        """
    # pylint: disable=maybe-no-member
    
    spreadsheet_id = SPREADSHEET_ID
    value_input_option = "USER_ENTERED"
    try:

        service = sheets_service
        
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input_option, body=body).execute()
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error




# Use a service account
cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()


## COGER TOTOS LOS PAYOUTS DE FIREBASE
payouts = db.collection('accountingItems'
                       ).where(u'context.account', u'==',OAN_account
                              ).where('concept','==','payout shopify').stream()

mappedFields = {
    'concept': 'concept',
    'vat': 'vat',
    'description':'description',
    'amount':'amount',
    'execution_date': 'executedAt',
    'user_in_charge': 'responsible',
    'account_id': 'originAccountingAccount',
    'code':'code',
    'type':'type',
    'target_id': 'targetAccountingAccount',
    'vat_amount': 'vatAmount'   
}
mappedContextFields = {
    'creation_date': 'createdAt',
    'creator_user':'createdBy',
    'id':'id',
    'origin': 'createdByType'
}

def convertData(acc):
    accItem = dict()
    accKeys = acc.keys()
    tag = None
    if acc["tags"]:
        tag = "spain" if "spain" in acc["tags"] else "benin" if "benin" in acc["tags"] else ""
    accItem["place"] = tag
    if 'files' in accKeys:
        if acc['files']:
            accItem['receipt'] = acc['files'][0]['downloadUrl']
        else:
            accItem['receipt'] = None
    for p in ['project','phase','intervention']:
        if 'origin'+p[0].capitalize()+p[1:] in accKeys:
            if acc['origin'+p[0].capitalize()+p[1:]] != '':
                accItem[p] = acc['origin'+p[0].capitalize()+p[1:]]
            elif 'target'+p[0].capitalize()+p[1:] in accKeys:
                accItem[p] = acc['target'+p[0].capitalize()+p[1:]]
            else:
                accItem[p] = ''
        elif 'target'+p[0].capitalize()+p[1:] in accKeys:
                accItem[p] = acc['target'+p[0].capitalize()+p[1:]]
        else:
            accItem[p] = ''
    for key in mappedFields:
        accItem[key] = acc[mappedFields[key]] if mappedFields[key] in accKeys else ""
    for key in mappedContextFields:
        accItem[key] = acc['context'][mappedContextFields[key]]
    return accItem


l_payouts = []
for payout in payouts:
    #print(payout)
    l_payouts.append(convertData(payout.to_dict()))

print('Ultimo payout en firebase {}'.format((pd.to_datetime(pd.DataFrame(l_payouts)['execution_date'].max())
                                            ).isoformat()[0:10]
                                           )
     )
date_min = (pd.to_datetime(pd.DataFrame(l_payouts)['execution_date'].max()) + pd.DateOffset(1)).isoformat()[0:10]
date_min

## LAST PAYOUT DRIVE
sheet = sheets_service.spreadsheets()
DATA_TO_PULL = 'Caja de Ingenieros' ##SSName
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                            range=DATA_TO_PULL).execute()
data = result.get('values', [])

df = pd.DataFrame(data[1:], columns=data[0])

### data treatment
df['IMPORTE'] = df['IMPORTE'].str[:-2].str.replace('.','',regex=False).str.replace(',','.',regex=False)
df['date'] = pd.to_datetime(df['FECHA'],format="%d/%m/%Y").apply(str).str[0:10]

maxDate = df['date'].max()
print('Ultimo registro en Diario 2022 {}'.format(maxDate))


r = requests.get(shop_url+"/shopify_payments/payouts.json",
                 params={
                     'limit':250,
                     'date_min':date_min,
                     "status": "paid"
                 })

payouts = r.json()['payouts']

if len(payouts) == 0:
    raise Exception('Already done with payouts')
      
payoutsExcel = [payout for payout in payouts if payout['date'] < maxDate]
      

print('Payouts a completar en excel {}'.format(str(len(payoutsExcel))))


counter_OAN = db.collection('info').document(OAN_account+'-accountingItems').get().to_dict()


#CREATION DATE TODAY
creation_date = datetime.today().isoformat()[:-3]+'Z'

## AÑADIRLOS A LA BBDD de FIREBASE
i=counter_OAN["counter"]+1
payouts2=[]

for payout in payoutsExcel[::-1]:
    d_conta={
        "amount":float(payout['amount']),
        "baseAmount":float(payout['amount']),
        "code":i,
        "concept":'payout shopify',
        "context":{
            "account":OAN_account,
            "createdAt":creation_date,
            "createdBy":"",
            "createdByType":"internalScript",
            "lastUpdateAt":creation_date,
            "lastUpdateBy":""
        },
        "description":'payout de banco stripe a caja de ingenieros',
        "executedAt":payout['date']+'T00:00:00.000Z',
        "files":None,
        "originAccountingAccount":BANCOSTRIPE,
        "originIntervention":GENERAL2022NIKARIT,
        "originPhase": None,
        "originProject": NIKARIT,
        "responsible":FIREBASEACCOUNT,
        "tags":["spain"],
        "targetAccountingAccount":CAJAINGENIEROS,
        "targetIntervention":GENERAL2022NIKARIT,
        "targetPhase": None,
        "targetProject": NIKARIT,
        "type":"movement",
        "vat": 0,
        "vatAmount":0   
    }
   
    i=i+1
    
    doc_acc = db.collection(u'accountingItems').add(d_conta)[1]
    doc_acc_id = doc_acc.id
    doc_acc.update({'context.id':doc_acc_id})
      
    ##get row in Caja de Ingenieros
    date = payout['date']
    importe = payout['amount']
    row = df[(df['date'] == date) &
           (df['IMPORTE'] == importe)].index[0]+2
    ##update google sheet to values
    update_values("'Caja de Ingenieros'!"+COLUMNAMATCHID+str(row),
              [
                  [doc_acc_id]
              ])


    d_conta['id'] = doc_acc_id
    payouts2.append(d_conta)

#actualizar el counter
db.collection('info').document(OAN_account+'-accountingItems').update({'counter':i})

#actualizar el saldo de los bancos
df_2 = pd.DataFrame(payouts2)
cantidad = df_2['amount'].sum()

bCajaIng = db.collection('accountingAccounts').document(CAJAINGENIEROS).get().to_dict()
print('Saldo en caja de ingenieros {}'.format(str(bCajaIng['currentAmount'])))

newAmountCaja = bCajaIng['currentAmount']+cantidad
db.collection('accountingAccounts').document(CAJAINGENIEROS).update({'currentAmount':newAmountCaja})
print('Saldo en caja de ingenieros {}'.format(str(newAmountCaja)))


bStripe = db.collection('accountingAccounts').document(BANCOSTRIPE).get().to_dict()
print('Saldo en Stripe {}'.format(str(bStripe['currentAmount'])))

newAmountStripe = bStripe['currentAmount']-cantidad
db.collection('accountingAccounts').document(BANCOSTRIPE).update({'currentAmount':newAmountStripe})
print('Saldo en Stripe {}'.format(str(newAmountStripe)))


#PRINTEAR POR PANTALLA PARA PODER MATCHEAR
print('\n'.join([' '.join([pay['id'],
                           pay['executedAt'][0:10],
                           str(pay['amount'])
                          ]) for pay in payouts2]))