"""
Este script coge los datos del los payouts de shopify y registra los payouts en Firebase e inserta el id en Diario2023

Aquellos payouts que no hayan sido contabilizados en Firebase y en el excel, los registra. No supera la ultima fecha de Diario2023.
Registra primero el movimiento en Firebase y luego inserta el id en la columna de matchId.

Ademas actualiza el saldo de Caja de Ingenieros y Stripe tras registrar todos los movimientos y el counter.

MEJORAS A IMPLEMENTAR:
-Que la funcion onCreate actualice también el counter

CAMBIOS
Unicamente matchear y no registrar donacion - a la espera de bton de registro mensual de donaciones.
"""

# Imports
import os
import string
from datetime import datetime, timedelta

import firebase_admin

## google cloud
import google.auth.transport.requests as grequests
import googleapiclient.discovery
import pandas as pd
import requests

# Firbease (pip install firebase_admin)
from firebase_admin import credentials, firestore
from google.oauth2 import service_account
from google.oauth2.id_token import fetch_id_token
from googleapiclient.errors import HttpError

shop_url = os.environ["SHOPIFY_ACCESS_URL"]

cloudCreds = os.environ["CLOUD_CREDS"]

OAN_account = "5Tv2u4n8BReebmKUNIuN"

BANCOSTRIPE = "iA9Pzv2CImjItzwCaQv0"
ESPAGNEGENERAL2024 = "FhnnK08Z47EkgPjZZ1NE"
NIKARITPROJECT = "0DmODGTOEiM5lg9SGx0J"
BANCOCAJAINGENIEROS = "vJbbj1kPxkcdXJyBOf1l"
DANIEL_USER_ID = "z5m936GA0t3vHM28QKhR"
BOT_USER_ID = "oXJJEfAEPxFYtdJ2pnaU"

CREATEDBYTYPE_SYSTEM = "system"

SPREADSHEET_ID = os.environ["DIARIO_2024"]
creation_date = datetime.today().isoformat()[:-3] + "Z"

URL_onCreateAccountingItemAPI = (
    "https://us-central1-oan-miong.cloudfunctions.net/onCreateAccountingItemAPI"
)


##connect to gsheets
## CONECTION TO DRIVE

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

cloudCredentials = service_account.Credentials.from_service_account_file(
    cloudCreds, scopes=SCOPES
)

sheets_service = googleapiclient.discovery.build(
    "sheets", "v4", credentials=cloudCredentials
)
sheet = sheets_service.spreadsheets()


##ACTUALIZAR UN VALOR EN GSHEET
def update_values(range_name, values):
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

        body = {"values": values}
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


##CREAR UN PAYOUT


def createPayoutDict(amount, date, i):
    randomId = db.collection("tmp").document().id
    data = {
        "amount": float(amount),
        "baseAmount": float(amount),
        "code": i,
        "concept": "payout shopify",
        "context": {
            "account": OAN_account,
            "createdAt": creation_date,
            "createdBy": BOT_USER_ID,
            "createdByType": CREATEDBYTYPE_SYSTEM,
            "id": randomId,
            "lastUpdateAt": creation_date,
            "lastUpdateBy": "",
        },
        "description": "payout de banco stripe a caja de ingenieros",
        "executedAt": date + "T00:00:00.000Z",
        "files": None,
        "originAccountingAccount": BANCOSTRIPE,
        "originIntervention": ESPAGNEGENERAL2024,
        "originPhase": None,
        "originProject": NIKARITPROJECT,
        "responsible": DANIEL_USER_ID,
        "tags": ["spain"],
        "targetAccountingAccount": BANCOCAJAINGENIEROS,
        "targetIntervention": ESPAGNEGENERAL2024,
        "targetPhase": None,
        "targetProject": NIKARITPROJECT,
        "type": "movement",
        "vat": 0,
        "vatAmount": 0,
    }
    accountItem = {"data": {"accountingItem": data}}
    return accountItem


# Use a service account
cred = credentials.Certificate(cloudCreds)
firebase_admin.initialize_app(cred)

db = firestore.client()


## COGER TOTOS LOS PAYOUTS DE FIREBASE
payoutsFirebase = (
    db.collection("accountingItems")
    .where("context.account", "==", OAN_account)
    .where("concept", "==", "payout shopify")
    .stream()
)
l_payouts = []
for payoutF in payoutsFirebase:
    # print(payout)
    l_payouts.append(payoutF.to_dict())

print(
    "Ultimo payout en firebase {}".format(
        (pd.to_datetime(pd.DataFrame(l_payouts)["executedAt"].max())).isoformat()[0:10]
    )
)
# date_min = (pd.to_datetime(pd.DataFrame(l_payouts)['executedAt'].max()) + pd.DateOffset(1)).isoformat()[0:10]
date_min = (
    pd.to_datetime(pd.DataFrame(l_payouts)["executedAt"].max()) + pd.Timedelta(days=1)
).isoformat()[0:10]

## LAST PAYOUT DIARIO
sheet = sheets_service.spreadsheets()
DATA_TO_PULL = "Caja de Ingenieros"  ##SSName
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=DATA_TO_PULL).execute()
data = result.get("values", [])
colsInSheet = data[0]
df_base = pd.DataFrame(data[1:], columns=colsInSheet)
df = df_base[df_base["FECHA"] != ""].copy()

### data treatment
df["Stripe"] = df["CONCEPTO"] == "TRANSF CTA DE:Stripe Technology"
df["NoMatcheado"] = (df["MATCH IDs"] == "") | (df["MATCH IDs"].isna())
df["IMPORTE"] = (
    df["IMPORTE"]
    .str[:-2]
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
)
df["date"] = pd.to_datetime(df["FECHA"], format="%d/%m/%Y").apply(str).str[0:10]
maxDate = df["date"].max()

print("Ultimo registro en Diario 2024 {}".format(maxDate))

## COGER COLUMNA DE MATCHING Id
alphabetList = list(string.ascii_uppercase)
matchRow = alphabetList[colsInSheet.index("MATCH IDs")]
cellMatchBeggining = "'Caja de Ingenieros'!" + matchRow

print("Columna de Caja de Ingenieros Diario {}\n".format(matchRow))

# GET TRANSACTIONS OF SHOPIFY

r = requests.get(
    shop_url + "/shopify_payments/payouts.json",
    params={"limit": 250, "date_min": date_min, "status": "paid"},
)

payouts = r.json()["payouts"]

if len(payouts) == 0:
    raise Exception("Already done with payouts")

payoutsShopifyToExcel = [payout for payout in payouts if payout["date"] <= maxDate]


print("Payouts que vienen de shopify {}".format(str(len(payoutsShopifyToExcel))))


print(
    "Payouts a completar en excel {}".format(
        str(len(df[(df["Stripe"]) & (df["NoMatcheado"])]))
    )
)

print("Ultimo dato {}".format(str(df["date"].max())), df.iloc[-8])


counter_OAN = (
    db.collection("info").document(OAN_account + "-accountingItems").get().to_dict()
)

bStripe = db.collection("accountingAccounts").document(BANCOSTRIPE).get().to_dict()
print(
    "Saldo en Stripe antes de empezar con payouts {}".format(
        str(bStripe["currentAmount"])
    )
)

## AÑADIRLOS A LA BBDD de FIREBASE


i = counter_OAN["counter"] + 1
payouts2 = []

##PREPARE CALL
# sent data to function
url = URL_onCreateAccountingItemAPI
auth_req = grequests.Request()
id_token = fetch_id_token(auth_req, url)
headers = {
    "Authorization": "Bearer {}".format(id_token),
    "Content-type": "application/json",
}

for payout in payoutsShopifyToExcel[::-1]:
    # CHECK IF IT IS ALREADY MATCHED
    ##get row in Caja de Ingenieros
    date = payout["date"]
    importe = payout["amount"]

    dfRow = df[
        (df["date"] == date)
        & (df["IMPORTE"] == importe)
        & (df["Stripe"])
        & (df["NoMatcheado"])
    ]

    if dfRow.shape[0] == 0:
        dateMinusOne = (
            datetime.fromisoformat(date[0:10]) - timedelta(days=1)
        ).isoformat()[0:10]
        dfRow = df[
            (df["date"] == dateMinusOne)
            & (df["IMPORTE"] == importe)
            & (df["Stripe"])
            & (df["NoMatcheado"])
        ]

        if dfRow.shape[0] == 0:
            datePlusOne = (
                datetime.fromisoformat(date[0:10]) + timedelta(days=1)
            ).isoformat()[0:10]
            dfRow = df[
                (df["date"] == datePlusOne)
                & (df["IMPORTE"] == importe)
                & (df["Stripe"])
                & (df["NoMatcheado"])
            ]

            if dfRow.shape[0] == 0:
                print("error in {} {}".format(date, importe))
                continue

    # SEND TO onCreateAccountingItemAPI
    # create accounting item
    accountItem = createPayoutDict(payout["amount"], payout["date"], i)

    # sent data to function
    r = grequests.requests.post(url, json=accountItem, headers=headers)

    # UPDATE DIARIO
    # Id
    accountItemId = accountItem["data"]["accountingItem"]["context"]["id"]

    row = dfRow.index[0] + 2
    ##update google sheet to values
    update_values(cellMatchBeggining + str(row), [[accountItemId]])

    i = i + 1
    # SAVE TO CHECK
    payouts2.append(accountItem["data"])

# actualizar el counter
db.collection("info").document(OAN_account + "-accountingItems").update({"counter": i})

df_payouts2 = pd.DataFrame(payouts2)
df_payouts2.to_csv("payoutsHechos.csv")

bStripe = db.collection("accountingAccounts").document(BANCOSTRIPE).get().to_dict()
print("Saldo en Stripe despues de payouts{}".format(str(bStripe["currentAmount"])))
