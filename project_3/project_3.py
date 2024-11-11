# %%
# Imports
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
import random
import seaborn as sns
import scipy.stats as stats
import uuid
from bson import ObjectId
from cassandra.cluster import Cluster
from datetime import datetime
from io import StringIO
from geopy import Nominatim
from meteostat import Hourly, Stations, Point
from pymongoarrow.api import write
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from scipy.fft import dct, idct

# Use a ggplot theme when plotting
plt.style.use("ggplot")

# %%
# PySpark set up

# Set environment variables to make PySpark work
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
os.environ["PYSPARK_HADOOP_VERSION"] = "without"

# Spark set up
spark = (
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

keyspace = "ind320_project"

# %%
# Cassandra set up
cluster = Cluster(["localhost"], port=9042)
session = cluster.connect()
keyspace = "ind320_project"

# Creating a keyspace in Cassandra
session.execute(
    "CREATE KEYSPACE IF NOT EXISTS"
    + " "
    + keyspace
    + " "
    + "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
)

session.set_keyspace(
    keyspace
)  # Setting the keyspace to be able to retrieve my tables

# %%
# Connecting to MongoDB
url = (
    "mongodb+srv://medvetslos:"
    + json.load(open("../../.nosync/mongoDB.json"))["pwd"]
    + "@ind320-project.lunku.mongodb.net/?retryWrites=true&w=majority&appName=IND320-project"
)

mdb_client = MongoClient(url, server_api=ServerApi("1"))

try:
    mdb_client.admin.command("ping")
    print("Pinged your deployment. Successfully connected to MongoDB.")
except Exception as exceptionMsg:
    print(exceptionMsg)

database = mdb_client["IND320-project"]  # Retrieving MongoDB collections
municipalities = database[
    "municipalities"
]  # Access the "muncipalities" collection

# %%
# Functions from assignment 1:


# Creating a view to Cassandra with PySpark
def create_cassandra_view(view_name: str, table_name: str, keyspace_name: str):
    spark.read.format("org.apache.spark.sql.cassandra").options(
        table=table_name, keyspace=keyspace_name
    ).load().createOrReplaceTempView(view_name)
    print(f"View '{view_name}' created.")


# %%
# Retrieve consumption table from Cassandra
create_cassandra_view("consumption_view", "consumption", keyspace)
df_consumption = spark.sql("SELECT * FROM consumption_view").toPandas()

# %% [markdown]
# ## Pivoting

# %%
df_municipalities = pd.DataFrame(list(database["municipalities"].find()))
df_consumption = df_consumption.sort_values("hourdk")
df_consumption.head()

# %%
q = df_consumption.groupby("municipalityno").sum("consumptionkwh").reset_index()
p = df_consumption.groupby("branche").sum("consumptionkwh").reset_index()
p.head()

# %% [markdown]
# ### Grouping/pivot function

# %%
df_consumption.pivot_table(
    index="municipality", values="consumptionkwh", aggfunc="mean"
)

# %%
df_consumption.shape

# %%
str.capitalize("eGSD")


# %%
def plot_results(df: pd.DataFrame, x: str, y: str):
    # Optional: implement interactivity for municipality case,
    # choose n top results.

    agg_method = y.split("_")[0]
    fig = px.bar(
        df,
        x,
        y,
        title=f"{str.capitalize(agg_method)} electricity consumption across '{x}'",
    )

    if df.shape[0] > 80:
        fig.update_xaxes(tickangle=45, tickfont=dict(size=8))

    fig.show()


# %%
def consumption_group_pivot(
    df: pd.DataFrame, group_col="municipality", agg_method="mean", plot=True
):
    response = "consumptionkwh"

    if group_col not in ["municipality", "branche"]:
        raise KeyError(f"Column '{group_col}' is not in the DataFrame.")

    df_agg = df.pivot_table(
        index=group_col, values=response, aggfunc=agg_method
    ).reset_index()

    df_agg = df_agg.sort_values(response, ascending=False)

    # Depending on agg_method, append agg_method_ to response
    agg_response = agg_method + "_" + response
    df_agg = df_agg.rename(columns={response: agg_response})

    if plot:
        plot_results(df_agg, group_col, agg_response)
        # match group_col:
        #     case "municipality":
        #         plot_results(df_agg, "municipality", agg_response)
        #     case "branche":
        #         plot_results(df_agg, "branche", agg_response)

    display(df_agg)

    return df_agg


# %%
p = consumption_group_pivot(df_consumption, "municipality", agg_method="mean")

# %% [markdown]
# ### Grouped barplot

# %%
import plotly.graph_objects as go
from ipywidgets import interactive, Dropdown


def grouped_barplot(df: pd.DataFrame, main_group, sub_group):
    # Extend further to add a dropdown box to choose specific "branche" or
    # "municipality", or None if one wants all the information available.

    fig = px.histogram(
        df, x=main_group, y="consumptionkwh", color=sub_group, barmode="group"
    )
    fig.show()


# %%
grouped_barplot(df_consumption, "municipality", "branche")

# %%
grouped_barplot(df_consumption, "branche", "municipality")

# %% [markdown]
# ## Correlation

# %% [markdown]
# *mean wind speed across all of denmark*

# %%
create_cassandra_view("weather_view", "weather_data", keyspace)
df_weather = spark.sql("SELECT * FROM weather_view").toPandas()
df_gas = pd.DataFrame(list(database["gas"].find()))

# %%
# Syncing the gas prices and the average wind speeds
df_gas_prices = df_gas[["GasDay", "PurchasePriceDKK_kWh"]].sort_values(
    "GasDay", ascending=True
)

df_wspd = df_weather[["datetime", "wspd"]].copy()
df_wspd.set_index("datetime", inplace=True)
df_wspd = df_wspd[["wspd"]].resample("D").mean()
df_wspd.reset_index(inplace=True)
df_wspd = df_wspd[
    (df_wspd["datetime"] >= min(df_gas_prices["GasDay"]))
    & (df_wspd["datetime"] <= max(df_gas_prices["GasDay"]))
]
df_wspd.reset_index(drop=True, inplace=True)
df_wspd

# %%
print(df_wspd.shape, df_gas_prices.shape)


# %%
def dct_swc(
    ts_prices: pd.Series,
    ts_avg_wspd: pd.Series,
    swc_width=7,
    lag=0,
    dct_cutoff=0,
    dct_filter=False,
):
    # The function assumes that you have calculated the average wind speed
    # for each day of the period the gas prices are defined.

    # Lagging back the wind speed series and filling using back fill
    if lag > 0:
        ts_avg_wspd = ts_avg_wspd.shift(lag).bfill()

    # DCT
    if dct_filter:
        w = np.arange(0, len(ts_prices))

        # Perform DCT on the time series
        dct_prices = dct(ts_prices)
        dct_avg_wspd = dct(ts_avg_wspd)

        # Filter the DCT coefficients
        dct_prices[(w > dct_cutoff)] = 0
        dct_avg_wspd[(w > dct_cutoff)] = 0

        # Convert the filtered DCT to get back to the time domain
        ts_prices = pd.Series(idct(dct_prices))
        ts_avg_wspd = pd.Series(idct(dct_avg_wspd))

    # SWC
    # Calculating the sliding window correlation
    price_avg_wspd_swc = ts_prices.rolling(swc_width, center=True).corr(
        ts_avg_wspd
    )

    return price_avg_wspd_swc, ts_avg_wspd


# %%
price_wspd_swc, lag_avg_wspd = dct_swc(
    df_gas_prices["PurchasePriceDKK_kWh"],
    df_wspd["wspd"],
    lag=7,
    dct_cutoff=15,
    dct_filter=True,
)
# plt.plot(price_wspd_swc)
# plt.show()

# %%
fig, ax = plt.subplots(2, 1, figsize=(10, 7))

# Top plot, purchase price and (lagged) wind speeds

# First curve, top plot
ax[0].plot(
    df_wspd["datetime"], df_gas_prices["PurchasePriceDKK_kWh"], color="blue"
)
ax[0].set_ylabel("Purchase price (DKK)", color="blue")
ax[0].tick_params(axis="y", labelcolor="blue")

# Second curve, top plot
twin_ax0 = ax[0].twinx()
twin_ax0.plot(df_wspd["datetime"], lag_avg_wspd)
twin_ax0.set_ylabel("(Lagged) avg. wind speed", color="red")
twin_ax0.tick_params(axis="y", labelcolor="red")

# Bottom plot, sliding window correlation
ax[1].plot(df_wspd["datetime"], price_wspd_swc, color="blue")
ax[1].set_ylabel("Sliding window correlation")
ax[1].set_xlabel("Date")

plt.show()

# %% [markdown]
# __Discuss the figure and strategy to produce it (???)__

# %% [markdown]
# ## Forecasting

# %% [markdown]
#
