#to open cred files
import os

#Data transformation
import pandas as pd

#bigquery
import pandas_gbq
from google.cloud import bigquery

#FIREBASE
import firebase_admin

from firebase_admin import credentials
from firebase_admin import firestore

from dotenv import load_dotenv
load_dotenv('/Users/daniel/OAN/credentials/contoan/.env')

## OPEN CONECTION TO BIGQUERRY
client = bigquery.Client()
dataset_id = "{}.contoan".format(client.project)
dataset = bigquery.Dataset(dataset_id)

#OPEN CONECTION TO FIREBASE
filename=os.environ['FIREBASE_FILENAME']
cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()


#get all the collection
col_query = db.collection('Accounting').stream()

#save it in a list of dictionaries
accounts = []
for acc in col_query:
    accounts.append(acc.to_dict())
    
#convert to dataframe
accounting = pd.DataFrame(accounts)

def clean_execution(x):
    if(len(x['execution_date'])<15):
        doc_id = x['id']
        exec_date=x['execution_date']+'T14:14:58.593Z'
        
        return exec_date
    else:
        return x['execution_date']

accounting['execution_date']=accounting.apply(lambda x: clean_execution(x) ,axis=1)

def clean_vat_amount(x):
    doc_id = x['id']
    amount=x['amount']
    origin = x['origin']
    if origin =='script_dani':
        if x['vat']>0:
            vat=21
        else:
            vat=0
        #a=db.collection(u'Accounting').document(doc_id).update({'vat':vat})
    else:
        vat=x['vat']
        if not(vat >=0):
            vat=0
    vat_amount=amount*(1-1/(1+vat/100))

    #a=db.collection(u'Accounting').document(doc_id).update({'vat_amount':vat_amount})

    return pd.Series({'vat':vat,'vat_amount':vat_amount})

accounting[['vat','vat_amount']] = accounting.apply(lambda x: clean_vat_amount(x),axis=1)

#utc=True important if dates are in diferent formats
accounting['creation_date'] = pd.to_datetime(accounting['creation_date'],utc=True)
accounting['execution_date'] = pd.to_datetime(accounting['execution_date'],utc=True)

def getimages(x):
    if not (x):
        return ''
    elif(len(x)==0):
        return ''
    elif('download_url' in x[0].keys()):
        download_url = x[0]['download_url']
        if(download_url == None):
            return ''
        else:
            return download_url
    else:
        return ''
    
accounting['receipt']=accounting.images.apply(lambda x: getimages(x))

#and the order whe want to upload to bigquery
df = accounting[['concept', 'place', 'creation_date', 'vat', 'description', 'amount',
       'id', 'phase', 'creator_user', 'execution_date',
       'user_in_charge', 'origin', 'account_id', 'project', 'code', 'type',
       'intervention', 'target_id', 'vat_amount', 'receipt']]

table_id = "{}.accounting".format(dataset.dataset_id)

pandas_gbq.to_gbq(df, table_id, project_id=client.project, if_exists='replace')