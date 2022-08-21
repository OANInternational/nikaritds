'''
Este script coge los datos del excel de donantes y de las tres pestañas de los bancos de Diario2022
y contabiliza aquellas donaciones que no han sido contabilizadas.
Reconece las donaciones a partir del concepto del banco y el importe del movimiento. Filtra también aquella que no han sido
registradas, es decir las que no tienen matchId.

Registra primero el movimiento en Firebase y luego inserta el id en la columna de matchId

MEJORAS A IMPLEMENTAR:
Las columnas en donde se encuentra el MATCH ID estan hardcodeadas - dictSheetTabColum. Debería detectarse por si algun usuario inserta columnas en las hojas.
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
 
load_dotenv(dotenv_path='/Users/daniel/OAN/credentials/contoan/.env')

## Conect to GSHEETS
from oauth2client import file,client, tools
from googleapiclient import discovery
from httplib2 import Http

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
sheet = sheets_service.spreadsheets()
SPREADSHEET_ID = os.environ['DIARIO_2022']
DONANTES_ID = os.environ['DONANTES_ID']
OANACCOUNT = "5Tv2u4n8BReebmKUNIuN"

CAJAINGENIEROS = "vJbbj1kPxkcdXJyBOf1l"
BANCOSANTANDER = "yZMcgtHOnl6dIudDQMru"
BANCOPAYPAL = "yfy4cPxkmFwIYVnoWD0A"
USERDANI = "z5m936GA0t3vHM28QKhR"
ADMINGENERAL = "4zcptWXv2IqQFkIMz2MP"
GENERAL2022 = "cTJa8TovRmSJEdu5stdc"
DONACIONESACCOUNT = "I6vsoTqCKFAS1AK09qzW"


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


##ACESS TO FIREBASE
filename=os.environ['FIREBASE_MIONG_FILENAME']
# Use a service account
cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()

## meses dict

meses = {
    1:'enero',
    2:'febrero',
    3:'marzo',
    4:'abril',
    5:'mayo',
    6:'junio',
    7:'julio',
    8:'agosto',
    9:'septiembre',
    10:'octubre',
    11:'noviembre',
    12: 'diciembre'
}

DATA_TO_PULL = "Sheet1" ##SSName
result = sheet.values().get(spreadsheetId=DONANTES_ID,
                            range=DATA_TO_PULL).execute()
data = result.get('values', [])
dfDonantes = pd.DataFrame(data[1:], columns=data[0])
dfDonantes['cantidad'] = dfDonantes['cantidad'].apply(float)
dfDictDonantes = dfDonantes.T.to_dict()
donantes = [dfDictDonantes[a] for a in dfDictDonantes]

sheetsTabs = ['Banco Santander','Caja de Ingenieros','Paypal']
dataFrames = []

for tab in sheetsTabs:
    DATA_TO_PULL = tab ##SSName
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=DATA_TO_PULL).execute()
    data = result.get('values', [])
    df = pd.DataFrame(data[1:], columns=data[0])
    df['IMPORTE'] = df['IMPORTE'].str[:-2].str.replace('.','',regex=False).str.replace(',','.',regex=False)
    df['IMPORTE'] = df['IMPORTE'].apply(lambda x: 0 if x=="" else float(x))
    df['date'] = pd.to_datetime(df['FECHA'],format="%d/%m/%Y").apply(str).str[0:10]
    df['year'] = pd.DatetimeIndex(df['date']).year
    df['month'] = pd.DatetimeIndex(df['date']).month
    df['month'] = df['month'].apply(lambda x: meses[x])
    df['year month'] = df['year'].apply(str) + " " + df['month']
    dataFrames.append(df[['date','year month','IMPORTE','CONCEPTO','MATCH IDs']])

bancos = [BANCOSANTANDER,CAJAINGENIEROS,BANCOPAYPAL]
dictDf = dict(zip(bancos,dataFrames))
dictSheetTab = dict(zip(bancos,sheetsTabs))
dictSheetTabColum = dict(zip(bancos,["F","G","AP"]))

##GEtInfoOAN

counter_OAN = db.collection('info').document(OANACCOUNT+'-accountingItems').get().to_dict()
counter_OAN["counter"]

##LOOP POR TODOS LOS DONANTEStoday = datetime.today()
today = datetime.today()
todayIso = today.isoformat()[:-3]+"Z"
i=counter_OAN["counter"]+1
donaciones2=[]
nRegistros = 0
for donante in donantes:
    nom = donante['nombre']
    importe = donante["cantidad"]
    banco = donante['banco']
    sheetTab = dictSheetTab[banco]
    sheetTabColumn = dictSheetTabColum[banco]
    df = dictDf[banco]
    porRegistrar = df[(df['CONCEPTO'].str.contains(donante["conceptoDiario"],case=False,regex=False)) &
                       ((df['MATCH IDs'].isna()) | (df['MATCH IDs'] == "")) &
                        (df['IMPORTE'] == donante["cantidad"])][['date','year month']].reset_index()
    
    if porRegistrar.shape[0] == 0:
        continue
    
    porRegistrar['concepto'] = "Donación "+nom+" "+porRegistrar['year month']
    registrar = porRegistrar.T.to_dict()
    for r in registrar:
        concept = registrar[r]['concepto']
        exDate = datetime.fromisoformat(registrar[r]['date']).isoformat()+".000Z"
        row = registrar[r]['index']+2
        don_conta={
                    "amount":importe,
                    "baseAmount":importe,
                    "code":i,
                    "concept":concept,
                    "context":{
                        "account":OANACCOUNT,
                        "createdAt":todayIso,
                        "createdBy":USERDANI,
                        "createdByType":"user",
                        "lastUpdateAt":todayIso,
                        "lastUpdateBy":""
                    },
                    "description":concept.replace("ó","o"),
                    "executedAt":exDate,
                    "files":None,
                    "originAccountingAccount":DONACIONESACCOUNT,
                    "originIntervention":None, 
                    "originPhase": None,
                    "originProject": None,
                    "responsible":USERDANI,
                    "tags":["spain"],
                    "targetAccountingAccount":banco,
                    "targetIntervention":GENERAL2022, 
                    "targetPhase": None,
                    "targetProject": ADMINGENERAL,
                    "type":"income",
                    "vat": 0,
                    "vatAmount":0   
                }
        
        i=i+1

        doc_acc = db.collection(u'accountingItems').add(don_conta)[1]
        doc_acc_id = doc_acc.id
        doc_acc.update({'context.id':doc_acc_id})

        ##update google sheet to values
        update_values("'"+sheetTab+"'!"+sheetTabColumn+str(row),
                  [
                      [doc_acc_id]
                  ])

        don_conta['context']['id'] = doc_acc_id
        donaciones2.append(don_conta)
        nRegistros = nRegistros+1
        
if nRegistros == 0:
     print('Todas las donaciones han sido registradas')
    
else:
    for j, don in enumerate(donaciones2):
        print('Registro {}\n{}\n'.format(j,don["concept"]+" id: "+don["context"]["id"]))

