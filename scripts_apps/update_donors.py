import os
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq as pd_gbq
from firebase_admin import credentials, firestore, initialize_app
from google.cloud import bigquery

filename = os.environ["FIREBASE_FILENAME"]
cred = credentials.Certificate(filename)
firebase = initialize_app(cred)

db = firestore.client()

OAN_account = "5Tv2u4n8BReebmKUNIuN"

donors = db.collection("donors").stream()

l_donors = []
for donorF in donors:
    # print(payout)
    l_donors.append(donorF.to_dict())


df_donors = pd.DataFrame(l_donors)

df_recurrentConfig = df_donors["recurrentConfig"].apply(pd.Series)
df_recurrentConfig = df_recurrentConfig.drop(columns=[0])

mapper = {
    "targetAccount": "recurrentConfigTargetAccount",
    "concept": "recurrentConfigConcept",
    "dayOfTheMonth": "recurrentConfigDayOfTheMonth",
    "amount": "recurrentConfigAmount",
}

df_recurrentConfig = df_recurrentConfig.rename(mapper, axis=1)
df_donors = df_donors.join(df_donors["context"].apply(pd.Series))
df_donors = df_donors.join(df_recurrentConfig)
df_donors = df_donors.drop(columns=["context"])
df_donors = df_donors.drop(columns=["recurrentConfig"])
df_donors = df_donors.drop(columns=["customFields"])

queryDonors = """
            SELECT *
            FROM `oan-miong.firestore_export.donorsView`
            """
df_donorsGbq = pd_gbq.read_gbq(queryDonors)


df_join_new = pd.merge(
    df_donors, df_donorsGbq["id"], how="left", on="id", indicator=True
)
df_new_donors = df_join_new[df_join_new["_merge"] == "left_only"].copy()
df_new_donors["action"] = "created"
df_new_donors["category"] = None
df_new_donors["timestamp"] = datetime.now().isoformat()
df_new_donors = df_new_donors.drop(columns=["_merge"])
df_new_donors.shape

df_donors_not_new = df_join_new[df_join_new["_merge"] == "both"].copy()
df_donors_not_new = df_donors_not_new.drop(columns=["_merge"])
df_merged = pd.merge(
    df_donors_not_new,
    df_donorsGbq[["id", "lastUpdateAt"]],
    how="inner",
    on="id",
    suffixes=("", "_gbq"),
)
# Filtrar registros donde 'lastUpdateAt_not_new' es más reciente
updated_records = df_merged[
    (df_merged["lastUpdateAt"] > df_merged["lastUpdateAt_gbq"])
    | (df_merged["lastUpdateAt_gbq"].isna() & ~df_merged["lastUpdateAt"].isna())
]

# Selecciona solo las columnas de df_donors_not_new
updated_records = updated_records[df_donors_not_new.columns]
updated_records["action"] = "updated"
updated_records["category"] = None
updated_records["timestamp"] = datetime.now().isoformat()
updated_records.shape

df_new_donorsGbq = pd.concat([df_donorsGbq, df_new_donors, updated_records])

print(
    [
        "actuales",
        df_donorsGbq.shape,
        "nuevos",
        df_new_donors.shape,
        "actualizados",
        updated_records.shape,
    ]
)

df_new_donorsGbq["recurrentConfigAmount"] = df_new_donorsGbq[
    "recurrentConfigAmount"
].astype(float)
df_new_donorsGbq["percentage"] = df_new_donorsGbq["percentage"].astype(float)
df_new_donorsGbq["isRecurrent"] = (
    df_new_donorsGbq["isRecurrent"].fillna(False).astype(bool)
)
df_new_donorsGbq["recurrentConfigDayOfTheMonth"] = df_new_donorsGbq[
    "recurrentConfigDayOfTheMonth"
].apply(lambda x: int(x) if x > 0 else None)

# %%
# Ajusta el DataFrame para cumplir con el esquema
df_new_donorsGbq["tags"] = df_new_donorsGbq["tags"].apply(
    lambda x: x.tolist() if isinstance(x, np.ndarray) else x
)
df_new_donorsGbq["tags"] = df_new_donorsGbq["tags"].apply(
    lambda x: x if isinstance(x, list) else [x]
)

df_new_donorsGbq["tags"] = [[] for a in range(df_new_donorsGbq.shape[0])]

orden = [
    "id",
    "action",
    "timestamp",
    "account",
    "category",
    "createdByType",
    "createdBy",
    "lastUpdateBy",
    "createdAt",
    "lastUpdateAt",
    "name",
    "lastName",
    "partner",
    "email",
    "dni",
    "region",
    "province",
    "key",
    "percentage",
    "nature",
    "tags",
    "isRecurrent",
    "recurrentConfigAmount",
    "recurrentConfigTargetAccount",
    "recurrentConfigDayOfTheMonth",
    "recurrentConfigConcept",
]

df_new_donorsGbq = df_new_donorsGbq[orden]

# Configura el cliente de BigQuery
client = bigquery.Client(project="oan-miong")

# Define la tabla destino
table_id = "oan-miong.firestore_export.donors"

table = client.get_table(table_id)

schema = [
    {
        "name": schema_field.name,
        "type": schema_field.field_type,
        "mode": schema_field.mode,
    }
    for schema_field in table.schema
]

# Configura el job de carga
job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_TRUNCATE",  # Reemplaza la tabla si ya existe
)

# Sube el DataFrame a BigQuery
job = client.load_table_from_dataframe(
    df_new_donorsGbq, table_id, job_config=job_config
)

# Espera a que termine el job
job.result()

print(f"Tabla cargada exitosamente a {table_id}.")
