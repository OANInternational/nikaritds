#to open cred files
import os

#Data transformation
import pandas as pd

#bigquery
from google.cloud import bigquery

#FIREBASE
import firebase_admin

from firebase_admin import credentials
from firebase_admin import firestore

from dotenv import load_dotenv
load_dotenv('/Users/daniel/OAN/credentials/contoan/.env')

print('beginning etl of accounting ... ')

## OPEN CONECTION TO BIGQUERRY
bgq_client = bigquery.Client()
dataset_id = "{}.contoan".format(bgq_client.project)
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
        elif x['account_id']=="I6vsoTqCKFAS1AK09qzW":
            vat=0
    vat_amount=amount*(1-1/(1+vat/100))

    #a=db.collection(u'Accounting').document(doc_id).update({'vat_amount':vat_amount})

    return pd.Series({'vat':vat,'vat_amount':vat_amount})

accounting[['vat','vat_amount']] = accounting.apply(lambda x: clean_vat_amount(x),axis=1)

#utc=True important if dates are in diferent formats
accounting['creation_date'] = pd.to_datetime(accounting['creation_date'],utc=True)
accounting['execution_date'] = pd.to_datetime(accounting['execution_date'],utc=True)

def getimages(x):
    receipt = ''
    if not (x):
        return receipt
    elif(len(x)==0):
        return receipt
    elif('drive_url' in x[0].keys()):
        download_url = x[0]['drive_url']
        if(download_url != None):
            return download_url
    elif('download_url' in x[0].keys()):
        download_url = x[0]['download_url']
        if(download_url != None):
            return download_url
    else:
        return ''
    
    return receipt
    
accounting['receipt']=accounting.images.apply(lambda x: getimages(x))

#and the order whe want to upload to bigquery
df = accounting[['concept', 'place', 'creation_date', 'vat', 'description', 'amount',
       'id', 'phase', 'creator_user', 'execution_date',
       'user_in_charge', 'origin', 'account_id', 'project', 'code', 'type',
       'intervention', 'target_id', 'vat_amount', 'receipt']]

table_id = "{}.accounting".format(dataset.dataset_id)
proj = bgq_client.project

df.to_gbq(table_id,proj,if_exists='replace')

print('done.')