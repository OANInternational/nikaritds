'''
Este script coge los datos del excel de donantes y de las tres pestañas de los bancos de Diario2022
y contabiliza aquellas donaciones que no han sido contabilizadas.
Reconece las donaciones a partir del concepto del banco y el importe del movimiento. Filtra también aquella que no han sido
registradas, es decir las que no tienen matchId.

Registra primero el movimiento en Firebase y luego inserta el id en la columna de matchId

MEJORAS A IMPLEMENTAR:
-A revisar
'''

# Imports
import pandas as pd

import requests
import yaml
import json
import os

import string

from datetime import datetime

#Firbease (pip install firebase_admin)
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

#Gsheets
import googleapiclient.discovery
from google.oauth2 import service_account
## google cloud
import google.auth.transport.requests as grequests
from google.oauth2.id_token import fetch_id_token

from dotenv import load_dotenv
 
load_dotenv(dotenv_path='/Users/daniel/OAN/credentials/miong/.env')



cloudCreds = os.environ['CLOUD_CREDS']

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
BOT_USER_ID = "oXJJEfAEPxFYtdJ2pnaU"

URL_onCreateAccountingItemAPI = "https://us-central1-oan-miong.cloudfunctions.net/onCreateAccountingItemAPI"

today = datetime.today()
todayIso = today.isoformat()[:-3]+"Z"

##connect to gsheets
## CONECTION TO DRIVE

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

cloudCredentials = service_account.Credentials.from_service_account_file(
        cloudCreds, scopes=SCOPES)

sheets_service = googleapiclient.discovery.build('sheets', 'v4', credentials=cloudCredentials)
sheet = sheets_service.spreadsheets()


##ACTUALIZAR UN VALOR EN GSHEET
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


##CREATE A DONATION DATA DICT
def createDonation(importe,concept,exDate,banco,i):
    randomId = db.collection('tmp').document().id
    
    data={
            "amount":importe,
            "baseAmount":importe,
            "code":i,
            "concept":concept,
            "context":{
                "account":OANACCOUNT,
                "createdAt":todayIso,
                "createdBy":BOT_USER_ID,
                "createdByType":"user",
                "id":randomId,
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
    
    accountItem = {
                    "data": data
                    }
    return accountItem


##ACESS TO FIREBASE
# Use a service account
cred = credentials.Certificate(cloudCreds)
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

##GET DONATIONS DATA
DATA_TO_PULL = "Sheet1" ##SSName
result = sheet.values().get(spreadsheetId=DONANTES_ID,
                            range=DATA_TO_PULL).execute()
data = result.get('values', [])
dfDonantes = pd.DataFrame(data[1:], columns=data[0])
##TRANSFORM DATA
dfDonantes['cantidad'] = dfDonantes['cantidad'].apply(float)

##CONVERT TO DICT
dfDictDonantes = dfDonantes.T.to_dict()
donantes = [dfDictDonantes[a] for a in dfDictDonantes]


### GET DATA FROM 3 TABS OF DIARIO2022
sheetsTabs = ['Banco Santander','Caja de Ingenieros','Paypal']
dataFrames = []
matchRows = []

for tab in sheetsTabs:
    DATA_TO_PULL = tab ##SSName
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=DATA_TO_PULL).execute()
    data = result.get('values', [])
    colsInSheet=data[0]
    df = pd.DataFrame(data[1:], columns=colsInSheet)
    df['IMPORTE'] = df['IMPORTE'].str[:-2].str.replace('.','',regex=False).str.replace(',','.',regex=False)
    df['IMPORTE'] = df['IMPORTE'].apply(lambda x: 0 if x=="" else float(x))
    df["NoMatcheado"] = (df['MATCH IDs'].isna()) | (df['MATCH IDs'] == "")
    df['date'] = pd.to_datetime(df['FECHA'],format="%d/%m/%Y").apply(str).str[0:10]
    df['year'] = pd.DatetimeIndex(df['date']).year
    df['month'] = pd.DatetimeIndex(df['date']).month
    df['month'] = df['month'].apply(lambda x: meses[x])
    df['year month'] = df['year'].apply(str) + " " + df['month']
    dataFrames.append(df[['date','year month','IMPORTE','CONCEPTO','MATCH IDs',"NoMatcheado"]])
    
    ## COGER COLUMNA DE MATCHING Id
    alphabetList = list(string.ascii_uppercase)+['A'+b for b in list(string.ascii_uppercase)]
    matchRow=alphabetList[colsInSheet.index("MATCH IDs")]
    matchRows.append(matchRow)

bancos = [BANCOSANTANDER,CAJAINGENIEROS,BANCOPAYPAL]
dictDf = dict(zip(bancos,dataFrames))
dictSheetTab = dict(zip(bancos,sheetsTabs))
dictSheetTabColum = dict(zip(bancos,matchRows))

##GEtInfoOAN of last counter

counter_OAN = db.collection('info').document(OANACCOUNT+'-accountingItems').get().to_dict()
counter_OAN["counter"]

##LOOP POR TODOS LOS DONANTES
#Initiate variables
i=counter_OAN["counter"]+1
donaciones2=[]
nRegistros = 0

##PREPARE CALL
#sent data to function
url = URL_onCreateAccountingItemAPI
auth_req = grequests.Request()
id_token = fetch_id_token(auth_req, url)
headers = {"Authorization":"Bearer {}".format(id_token),
           'Content-type': 'application/json'}

for donante in donantes:
    ## DATOS DEL DONANTE
    nom = donante['nombre']
    importe = donante["cantidad"]
    banco = donante['banco']
    sheetTab = dictSheetTab[banco]
    sheetTabColumn = dictSheetTabColum[banco]
    
    ## MIRAR SI HAY ALGUN REGISTRO SIN MATCHEAR Y SIN REGISTRAR
    df = dictDf[banco]
    porRegistrar = df[(df['CONCEPTO'].str.contains(donante["conceptoDiario"],case=False,regex=False)) &
                       (df["NoMatcheado"]) &
                        (df['IMPORTE'] == donante["cantidad"])][['date','year month']].reset_index()
    
    if porRegistrar.shape[0] == 0:
        continue
    
    porRegistrar['concepto'] = "Donación "+nom+" "+porRegistrar['year month']
    registrar = porRegistrar.T.to_dict()
    
    ##SI HAY ALGUNO SIN REGISTRAR REGISTRARLO
    for r in registrar:
        concept = registrar[r]['concepto']
        exDate = datetime.fromisoformat(registrar[r]['date']).isoformat()+".000Z"
        row = registrar[r]['index']+2
        ##DATOS DEL REGISTRO
        accountingItem = createDonation(importe,concept,exDate,banco,i)
        accountingItemId = accountingItem['data']['context']['id']
        ##AUMENTAR EL CONTANDOR
        i=i+1
        
        #sent data to function
        r = grequests.requests.post(url, json=accountingItem,headers=headers)

        ##update google sheet to values
        update_values("'"+sheetTab+"'!"+sheetTabColumn+str(row),
                  [
                      [accountingItemId]
                  ])
        
        ##GUARDAR LA INFO DE LOS YA REGISTRADOS
        donaciones2.append(accountingItem['data'])
        nRegistros = nRegistros+1
        
#actualizar el counter
db.collection('info').document(OANACCOUNT+'-accountingItems').update({'counter':i})

if nRegistros == 0:
     print('Todas las donaciones ya habían sido registradas')
    
else:
    for j, don in enumerate(donaciones2):
        print('Registro {}\n{}\n'.format(j,don["concept"]+" id: "+don["context"]["id"]))

