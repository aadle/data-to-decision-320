
import json
import numpy as np
import os 
import pandas as pd
import plotly.express as px
import writer as wf
import writer.ai

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession


# More documentation is available at https://dev.writer.com/framework

def _start_spark():
    # Set environment variables to make PySpark work
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/"
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
    os.environ["PYSPARK_HADOOP_VERSION"] = "without"

    # Spark set up
    spark_session = (
        SparkSession.builder.appName("SparkCassandraApp")
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
        )
        .config("spark.cassandra.connection.host", "localhost")
        .config(
            "spark.sql.extensions",
            "com.datastax.spark.connector.CassandraSparkExtensions",
        )
        .config(
            "spark.sql.catalog.mycatalog",
            "com.datastax.spark.connector.datasource.CassandraCatalog",
        )
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.task.maxFailures", "10")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    return spark_session


def _create_cassandra_view(
    view_name: str,
    table_name: str,
    spark_obj,
    keyspace_name: str = "ind320_project",
):
    spark_obj.read.format("org.apache.spark.sql.cassandra").options(
        table=table_name, keyspace=keyspace_name
    ).load().createOrReplaceTempView(view_name)
    print(f"View '{view_name}' created.")


def _get_cassandra_table(
    view_name: str,
    table_name: str,
    spark_session: SparkSession,
    keyspace_name: str = "ind320_project",
):
    _create_cassandra_view(
        view_name=view_name,
        table_name=table_name,
        spark_obj=spark_session,
        keyspace_name=keyspace_name,
    )

    query = f"SELECT * FROM {view_name}"
    cassandra_df = spark_session.sql(query).toPandas().\
        sort_values("hourutc", ascending=True)

    print(f"Table {table_name} have been retrieved.")

    return cassandra_df


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
## ---- Main page ----

### Main page update?

def _create_map(state):

    df_municipality = state["data"]["df_municipality"]

    fig = px.scatter_mapbox(df_municipality,
                         lat="Latitude",
                         lon="Longitude",
                         hover_name="Municipality",
                         hover_data=['LAU-1 code 1', 
                                     'Municipality', 'Administrative Center', 
                                     'Total Area (km²)', 
                                     'Population (2012-01-01)', 'Region', 
                                     'Latitude', 'Longitude', 'Price Area'
                                     ],
                         zoom = 4,
                         height=600, 
                         width=900,
                         )

    fig.update_layout(mapbox_style="open-street-map")
    state["map"] = fig


def _test_fig(state):
    np.random.seed(42)
    dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
    data = {
        'date': dates,
        'sales': np.random.normal(100, 15, len(dates)),
        'temperature': np.random.normal(25, 5, len(dates)),
        'category': np.random.choice(['A', 'B', 'C'], len(dates)),
        'region': np.random.choice(['North', 'South', 'East', 'West'], len(dates))
}
    df_1 = pd.DataFrame(data)

    # 1. Line plot with date
    fig1 = px.line(df_1, 
                   x='date', 
                   y='sales',
                   title='Daily Sales Over Time',
                   template='plotly_white', 
                   width=800,height=600,
                   )
    state["test_fig"] = fig1


def _create_main_plot(state):
    """
    payload: dataframe, municipality
    """
    current_source = state["main_page"]["data_source"]
    municipalityno = state["main_page"]["municipalityno"]
    municipality_name = state["muni_JSON"][str(municipalityno)]
    column = state["main_page"]["data_column"]
    
    df_data = state["data"][current_source].\
        query(f"municipalityno == {municipalityno}")[["hourutc", column]]
    
    fig = px.line(df_data,
                  x = "hourutc", 
                  y = column,
                  template="plotly_white",
                  width=800, height=600,
                  title=f"{municipality_name}") 

    state["main_fig"] = fig
    _get_prod_types(state, state["data"][current_source])


def _update_main_plot(state):
    current_source = state["main_page"]["data_source"]
    municipalityno = state["main_page"]["municipalityno"]
    municipality_name = state["muni_JSON"][str(municipalityno)]
    column = state["main_page"]["data_column"]
   

    df_data = state["data"][current_source].\
        query(f"municipalityno == {municipalityno}")[["hourutc", column]]
    
    fig = px.line(df_data,
                  x = "hourutc", 
                  y = column,
                  template="plotly_white",
                  width=800, height=600,
                  title=f"{municipality_name}") 

    state["main_fig"] = fig
    _get_prod_types(state, state["data"][current_source])

def _get_prod_types(state, df):
    """
    payload: dataframe
    """

    ignore_cols = [
        "id", "hourdk", "hourutc", "municipality", "municipalityno", # general 
        "branche", # prodcons
        "coco", "distance_to_station" # weather
            ] 

    col_names = df.columns.tolist()
    prod_types = {str(k): str(k).capitalize() for k in
        list(set(col_names)-set(ignore_cols))}
   
    state["prod_type"] = prod_types

def handle_municipality(state, payload):
    print(f"Changing to {state["muni_JSON"][payload]}")
    state["main_page"]["municipalityno"] = int(payload)
    _update_main_plot(state) 

def handle_data_source(state, payload):
    # Change such that the data is set.
    state["main_page"]["data_source"] = payload

def handle_data_column(state, payload):
    print(f"Column changing to {payload}")
    state["main_page"]["data_column"] = payload
    _update_main_plot(state) 

def _create_map_pie(state, payload):
    pass


def _get_municipality_JSON(state):
    # Get municipality from mongoDB

    muni_df = state["data"]["df_municipality"][["Municipality", "LAU-1 code 1"]]
    dict_mun = {f"{str(k)}": v for k, v in zip(muni_df['LAU-1 code 1'],
                                               muni_df['Municipality'])} 

    state["muni_JSON"] = dict_mun
    
## ---- Update functions ----

def update(state):
    _create_map(state)
    _test_fig(state)
    _create_main_plot(state)

def _update_map(state):
    pass

## ---- Initializations ---- 

spark_session = _start_spark()
database = _mongodb_connect()

# State object is a dictionary.
initial_state = wf.init_state(
    {
        # DataFrames - insert into "data": {} ? 
        "data": {
            # Data from Cassandra
            # "df_production": _get_cassandra_table(
            #     view_name = "production_view",
            #     table_name = "production",
            #     spark_session=spark_session,
            # ),
            # "df_prodcons": _get_cassandra_table(
            # view_name = "prodcons_view",
            # table_name = "prodcons",
            # spark_session=spark_session,
            # ),
            "df_weather": _get_cassandra_table(
                view_name = "weather_view",
                table_name = "weather",
                spark_session=spark_session,
            ),
            # "df_consumption": _get_cassandra_table(
            #     view_name = "consumption_view",
            #     table_name = "consumption",
            #     spark_session=spark_session,
            # ),

            # Data from MongoDB 
            "df_gas": _get_mongodb_collection(database, "gas"),
            "df_municipality": _get_mongodb_collection(database,
                                                       "municipalities"),
        },
        "titles": {"header_main": "woof"},
        "main_page": {
            "data_source": "df_weather",
            "municipalityno": "101",
            "data_column": "temp"
        },
    }
)

_get_municipality_JSON(initial_state)
_create_map(initial_state)
_create_main_plot(initial_state)
# update(initial_state)
