import writer as wf
import writer.ai
import pandas as pd
import json

from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient


# Welcome to Writer Framework! 
# This template is a starting point for your AI apps.
# More documentation is available at https://dev.writer.com/framework

def _load_data(path:str):
    df = pd.read_csv(path)
    df = df.astype({"hourutc": "datetime64[s]"})

    filename = path.split("/")[2]

    print(f"{filename} have been loaded.")
    print()
    print(df.columns.tolist())
    print()

    return df

def _mongodb_connect():
    url = (
        "mongodb+srv://medvetslos:"
        + json.load(open("../../../.nosync/mongoDB.json"))["pwd"]
        + "@ind320-project.lunku.mongodb.net/?retryWrites=true&w=majority&appName=IND320-project"
    )
    mdb_client = MongoClient(url, server_api=ServerApi("1"))

    try:
        mdb_client.admin.command("ping")
        print("Pinged your deployment. Successfully connected to MongoDB.")
    except Exception as exceptionMsg:
        print(exceptionMsg)

    database = mdb_client["IND320-project"]

    return database

def _get_mongodb_collection(mongodb, collection_name: str):
    df_mongodb = pd.DataFrame.from_records(
        [entry for entry in mongodb[collection_name].find({}, {"_id": 0})]
        )

    return df_mongodb



production = _load_data("../data/production.csv")
prodcons = _load_data("../data/prodcons.csv")
weather = _load_data("../data/weather.csv")
consumption = _load_data("../data/consumption.csv")

# Initialise the state
wf.init_state({
    "my_app": {
        "title": "AI STARTER"
    },

    "data": {
        # Cassandra tables in .csv format
        # "df_production": _load_data("../data/production.csv"),
        # "df_prodcons": _load_data("../data/prodcons.csv"),
        # "df_weather": _load_data("../data/weather.csv"),
        # "df_consumption": _load_data("../data/consumption.csv"),

        "df_production": production,
        "df_prodcons": prodcons,
        "df_weather": weather,
        "df_consumption": consumption,
    }
})
