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
load_dotenv('/Users/daniel/OAN/credentials/miong/.env')

print('beginning etl of accounting ... ')

#OPEN CONECTION TO FIREBASE
filename=os.environ['FIREBASE_MIONG_FILENAME']

## OPEN CONECTION TO BIGQUERRY
bgq_client = bigquery.Client()
dataset_id = "{}.oan".format(bgq_client.project)
dataset = bigquery.Dataset(dataset_id)

cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()

OAN_account = "5Tv2u4n8BReebmKUNIuN"

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

#get all the collection of OAN
col_query = db.collection('accountingItems').where(u'context.account', u'==',OAN_account).stream()

#save it in a list of dictionaries
accounts = []
for acc in col_query:
    accounts.append(convertData(acc.to_dict()))

#convert to dataframe
accounting = pd.DataFrame(accounts)

#make datetime
accounting['creation_date'] = pd.to_datetime(accounting['creation_date'],utc=True)
accounting['execution_date'] = pd.to_datetime(accounting['execution_date'],utc=True)

#and the order whe want to upload to bigquery
df = accounting[['concept', 'place', 'creation_date', 'vat', 'description', 'amount',
       'id', 'phase', 'creator_user', 'execution_date',
       'user_in_charge', 'origin', 'account_id', 'project', 'code', 'type',
       'intervention', 'target_id', 'vat_amount', 'receipt']].copy()

for a in df.columns:
    if a in ['concept', 'place', 'creation_date', 'description', 'phase', 'creator_user', 'execution_date',
       'user_in_charge', 'origin', 'account_id', 'project', 'type',
       'intervention', 'target_id', 'receipt','id']:
    
        df[a] = df[a].astype(str)
    elif a in ['code']:
        df[a] = df[a].apply(int)
    else:
        df[a] = df[a].apply(lambda x: float(x) if x 
                                               else 0.0 if x == ''
                                               else 0.0)
        
        


table_id = "{}.accounting".format(dataset.dataset_id)
proj = bgq_client.project

df.to_gbq(table_id,proj,if_exists='replace')

print('done.')