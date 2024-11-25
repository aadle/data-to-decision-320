import json
import numpy as np
import os 
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import writer as wf
import writer.ai

from scipy.fft import dct, idct
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from plotly.subplots import make_subplots


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

def _create_map(state):
    df_pivot = state["data"]["df_pivot"]

    map_fig = px.scatter_mapbox(
        df_pivot,
        lat="Latitude",
        lon="Longitude",
        hover_name="Municipality",
        hover_data=[
            'LAU-1 code 1', 
            'Municipality', 
            'Administrative Center', 
            'Total Area (km²)', 
            'Population (2012-01-01)', 
            'Region', 
            'Latitude', 
            'Longitude', 
            'Price Area'
            ],
        animation_frame=df_pivot["hourutc"].dt.strftime('%Y-%m-%d'),
        size="dot_size",
        opacity=0.8
    )

    state["map_fig"] = map_fig

def _create_line_plot(state):
    data_source = state["main_page"]["data_source"]
    data_column = state["main_page"]["data_column"]
    municipalityno = state["main_page"]["municipalityno"]
    municipality_name = state["muni_JSON"][str(municipalityno)]
    df = state["data"]["df_pivot"]
    df = df[df["municipalityno"] == int(municipalityno)]

    match data_source:
        case "df_production":
            title = f"Daily total {data_column} production in {municipality_name}"
        case "df_consumption":
            title = f"Daily total {data_column} consumption"
        case "df_weather":
            title = f"Daily mean {data_column} in {municipality_name}"
        case _:
            title = None

    line_fig = px.scatter(
        df,
        x="hourutc",
        y=data_column,
        title=title,
        animation_frame=df["hourutc"].dt.strftime('%Y-%m-%d'),
    )

    state["line_title"] = title
    state["line_fig"] = line_fig

def _create_main_plot(state):
    current_source = state["main_page"]["data_column"]
    municipalityno = state["main_page"]["municipalityno"]

    df_pivot = state["data"]["df_pivot"]
    df_line = df_pivot[df_pivot["municipalityno"] == int(municipalityno)]

    line_fig = state["line_fig"]
    map_fig = state["map_fig"]

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "mapbox"}, {"type": "xy"}]],
        vertical_spacing=0.1,
        subplot_titles=("", state["line_title"])
    )

    dct_cutoff=5
    w = np.arange(0, df_line.shape[0])
    dct_line = dct(df_line[current_source].values)
    dct_line[(w > dct_cutoff)] = 0

    df_line.loc[:, "idct"] = idct(dct_line)

    fig.add_trace(
        go.Scatter(
            x=df_line["hourutc"],
            y=df_line["idct"],
            mode="lines",
            line=dict(color="gray", width=30),
            showlegend=False,
            opacity=0.6,
        ),
        row=1, col=2
    )

    # Add static line trace of the series in its entirety
    fig.add_trace(
        go.Scatter(
            x=df_line["hourutc"],
            y=df_line[current_source],
            mode="lines",
            line=dict(color="blue"),
            showlegend=False
        ),
        row=1, col=2
    )

    # Add intial position of red dot
    fig.add_trace(
        line_fig.frames[0].data[0],
        row=1, col=2
    )

    # Add map traces
    for trace in map_fig.frames[0].data:
        fig.add_trace(trace, row=1, col=1)

    # Create animation frames
    frames = []
    for map_frame, dot_frame in zip(map_fig.frames, line_fig.frames):
        frame = go.Frame(
            data=[
                *map_frame.data,  # Map traces
                go.Scatter(  # Static line (doesn't change)
                    x=df_line['hourutc'],
                    y=df_line[current_source],
                    mode='lines',
                    line=dict(color='blue'),
                    # name='Total Production'
                ),
                go.Scatter(
                    x=df_line["hourutc"],
                    y=df_line["idct"],
                    mode="lines",
                    line=dict(color="gray", width=30),
                    showlegend=False,
                    opacity=0.6,
                ),
                go.Scatter(  # Moving dot
                    x=dot_frame.data[0].x,
                    y=dot_frame.data[0].y,
                    mode='markers',
                    marker=dict(color='red', size=12),
                    name='Current Value'
                )
            ],
            name=map_frame.name
        )
        frames.append(frame)

    # Update layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            zoom=5,
            center=dict(lat=df_pivot['Latitude'].mean(), 
                       lon=df_pivot['Longitude'].mean())
        ),
        updatemenus=[
            {
                "buttons": [
                    {
                        "args": [None, {"frame": {"duration": 200, "redraw": True},
                                      "fromcurrent": True,
                                      "transition": {"duration": 200}}],
                        "label": "▶️ Play",
                        "method": "animate"
                    },
                    {
                        "args": [[None], {"frame": {"duration": 0, "redraw": False},
                                        "mode": "immediate",
                                        "transition": {"duration": 0}}],
                        "label": "⏸ Pause",
                        "method": "animate"
                    }
                ],
                "direction": "left",
                "pad": {"r": 10, "t": 87},
                "showactive": True,
                "type": "buttons",
                "x": 0.1,
                "xanchor": "right",
                "y": 0,
                "yanchor": "top"
            }
        ],
        # Add slider
        sliders=[{
            "active": 0,
            "yanchor": "top",
            "xanchor": "left",
            "currentvalue": {
                "font": {"size": 20},
                "prefix": "Date: ",
                "visible": True,
                "xanchor": "right"
            },
            "transition": {"duration": 100, "easing": "cubic-in-out"},
            "pad": {"b": 10, "t": 50},
            "len": 0.9,
            "x": 0.1,
            "y": 0,
            "steps": [
                {
                    "args": [
                        [frame.name],
                        {
                            "frame": {"duration": 100, "redraw": True},
                            "mode": "immediate",
                            "transition": {"duration": 100}
                        }
                    ],
                    "label": frame.name,
                    "method": "animate"
                }
                for frame in frames
            ]
        }],
        # Other layout settings
        height=500,
        width=1000,
        showlegend=True,
        margin=dict(r=0, t=87, l=0, b=0)
    )

    # Add frames to the figure
    fig.frames = frames

    state["main_fig"] = fig

def _update_main_plot(state):
    _pivot_data(state)
    _create_map(state)
    _create_line_plot(state)
    _create_main_plot(state)

def _get_columns(state):
    data_source = state["main_page"]["data_source"]
    df = state["data"][data_source]

    ignore_cols = [
        "id", "hourdk", "hourutc", "municipality", "municipalityno", # general 
        "branche", # prodcons
        "coco", "distance_to_station", # weather
        "_id", # df_municipality
            ] 

    col_names = df.columns.tolist()
    columns = {str(k): str(k).capitalize() for k in
        list(set(col_names)-set(ignore_cols))}
   
    state["columns"] = columns 

def handle_municipality(state, payload):
    print(f"Changing to municipality to {state["muni_JSON"][payload]}")
    state["main_page"]["municipalityno"] = int(payload)
    _update_main_plot(state) 

def handle_data_source(state, payload):
    state["main_page"]["data_source"] = payload
    _get_columns(state)
    _update_main_plot(state)
    
def handle_data_column(state, payload):
    print(f"Column changing to {payload}")
    state["main_page"]["data_column"] = payload
    _update_main_plot(state)

def handle_map_click(state, payload):
    payload = payload[0]
    latitude = payload["lat"]
    longitude = payload["lon"]
    df_municipality = state["data"]["df_municipality"]

    chosen_point = df_municipality[
        (df_municipality["Latitude"] == latitude) & 
        (df_municipality["Longitude"] == longitude)
    ]

    municipalityno = str(chosen_point.iloc[0, :]["LAU-1 code 1"])
    handle_municipality(state, municipalityno)

def _create_map_pie(state, payload):
    pass

def _pivot_data(state):

    # Current DataFrame in use
    current_source = state["main_page"]["data_source"]

    # What column we want  to use
    data_column = state["main_page"]["data_column"]

    # Accessing the DataFrame
    df_data = state["data"][current_source]

    # Accessing the municipality DataFrame 
    df_municipality = state["data"]["df_municipality"]

    # Switch statement as we want to don't want the sum of weather observations
    match current_source:
        case "df_weather":
            df_pivot = (
                df_data.pivot_table(
                    index="hourutc",
                    values=data_column,
                    columns="municipalityno",)
                .resample("D", level="hourutc").mean()
            )
        case _:
            df_pivot = (
                df_data.pivot_table(
                    index="hourutc",
                    values=data_column,
                    columns="municipalityno",)
                .resample("D", level="hourutc").sum()
            )

    df_pivot_reset = df_pivot.reset_index()
    df_pivot = df_pivot_reset.melt(
        id_vars=["hourutc"],
        var_name="municipalityno",
        value_name=data_column
    )

    match current_source:
        case "df_weather":
            df_pivot["dot_size"] = (df_pivot[data_column] - 
                df_pivot[data_column].min()) / \
                ((df_pivot[data_column].max() - df_pivot[data_column].min())*10)
        case _:
            df_pivot["dot_size"] = np.sqrt(df_pivot[data_column])
    
    df_complete = df_municipality.merge(
        df_pivot,
        left_on="LAU-1 code 1",
        right_on="municipalityno",
    )

    state["data"]["df_pivot"] = df_complete
   
def _get_municipality_JSON(state):
    # Get municipality from mongoDB
    muni_df = state["data"]["df_municipality"][["Municipality", "LAU-1 code 1"]]
    dict_mun = {f"{str(k)}": v for k, v in zip(muni_df['LAU-1 code 1'],
                                               muni_df['Municipality'])} 
    state["muni_JSON"] = dict_mun
    

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
            "data_column": "temp",
        },
    }
)

_pivot_data(initial_state)
_get_municipality_JSON(initial_state)

_create_map(initial_state)
_create_line_plot(initial_state)
_create_main_plot(initial_state)

_get_columns(initial_state)
