import json
import numpy as np
import os 
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt 
import writer as wf
import statsmodels.api as sm

from scipy.fft import dct, idct
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from plotly.subplots import make_subplots

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

def _load_data(path:str):
    df = pd.read_csv(path)
    df = df.astype({"hourutc": "datetime64[s]"})
    df = df.sort_values("hourutc", ascending=True)

    filename = path.split("/")[2]

    print(f"{filename} have been loaded.")
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

## ---- Main page ----

def _create_map(state):
    df_pivot = state["data"]["df_pivot"]
    data_column = state["main_page"]["data_column"]

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
            'Price Area',
            data_column,
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
            title = f"Daily total consumption for {municipality_name}"
        case "df_weather":
            title = f"Daily mean {data_column} in {municipality_name}"
        case _:
            title = None
 
    dct_cutoff=5
    w = np.arange(0, df.shape[0])
    dct_line = dct(df[data_column].values)
    dct_line[(w > dct_cutoff)] = 0
    idct_line = idct(dct_line)

    df.loc[:, "idct"] = idct_line 

    # Creates the dot of the line plot
    dot_fig = px.scatter(
        df,
        x="hourutc",
        y="idct",
        animation_frame=df["hourutc"].dt.strftime('%Y-%m-%d'),
    )

    # line_fig = px.scatter(
    #     df,
    #     x="hourutc",
    #     y=data_column,
    #     title=title,
    #     animation_frame=df["hourutc"].dt.strftime('%Y-%m-%d'),
    # )
    #

    state["dct_vals"] = idct_line
    state["fig_title"] = title
    state["dot_fig"] = dot_fig 


def _create_main_plot(state):
    current_source = state["main_page"]["data_column"]
    municipalityno = state["main_page"]["municipalityno"]

    df_pivot = state["data"]["df_pivot"]

    df_line = df_pivot[df_pivot["municipalityno"] == int(municipalityno)]

    dot_fig = state["dot_fig"]
    map_fig = state["map_fig"]
    dct_vals = state["dct_vals"]

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "mapbox"}, {"type": "xy"}]],
        vertical_spacing=0.1,
        subplot_titles=("", state["fig_title"])
    )

    # Add static line trace of the original series in its entirety
    fig.add_trace(
        go.Scatter(
            x=df_line["hourutc"],
            y=df_line[current_source],
            mode="lines",
            line=dict(color="grey"),
            opacity=0.6,
            showlegend=False,
            fill="none",
        ),
        row=1, col=2
    )

    # Add static line trace of the DCT filtered series
    fig.add_trace(
        go.Scatter(
            x=df_line["hourutc"],
            y=dct_vals,
            mode="lines",
            line=dict(color="blue"),
            showlegend=False
        ),
        row=1, col=2
    )
    
    # Add intial position of red dot
    fig.add_trace(
        dot_fig.frames[0].data[0],
        row=1, col=2
    )
    
    # Add map traces
    for trace in map_fig.frames[0].data:
        fig.add_trace(trace, row=1, col=1)

    # Create animation frames
    frames = []
    for map_frame, dot_frame in zip(map_fig.frames, dot_fig.frames):
        frame = go.Frame(

            data=[
                *map_frame.data,  # Map traces
                go.Scatter(  # Static original line (doesn't change)
                    x=df_line['hourutc'],
                    y=df_line[current_source],
                    mode='lines',
                    line=dict(color='grey'),
                    opacity=0.6,
                    showlegend=False
                    # name='Total Production'
                ),
                go.Scatter( # Static DCT line 
                    x=df_line["hourutc"],
                    y=dct_vals,
                    mode="lines",
                    line=dict(color="blue"),
                    showlegend=False
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


def _create_pie_map(state):

    capital_coords = {
        "exchangeno_mwh": (59.9139, 10.7522), # Oslo
        "exchangese_mwh": (59.3327, 18.0656), # Stockholm
        "exchangege_mwh": (52.5200, 13.4050), # Berlin
        "exchangenl_mwh": (52.3676, 4.9041) # Amsterdam
    }

    df = _load_data("../data/prodcons.csv")

    df = df[[
        'hourutc',
        'exchangege_mwh',
        'exchangenl_mwh',
        'exchangeno_mwh',
        'exchangese_mwh',
    ]].sort_values("hourutc", ascending=True)

    df_pivot = df.pivot_table(index="hourutc").resample("D", level="hourutc").sum()
    df_pivot = df_pivot.reset_index()

    df_pivot_idx = df_pivot.melt(id_vars=["hourutc"], 
                             value_vars=['exchangege_mwh', 'exchangenl_mwh', 
                                         'exchangeno_mwh', 'exchangese_mwh'])
    df_pivot_idx = df_pivot_idx.rename(columns={"value": "exchange_mwh"})
    df_pivot_idx["latitude"] = df_pivot_idx["variable"].\
        map(lambda x: capital_coords[x][0])
    df_pivot_idx["longitude"] = df_pivot_idx["variable"].\
        map(lambda x: capital_coords[x][1])
    df_pivot_idx["abs_exchange_mwh"] = df_pivot_idx["exchange_mwh"].abs()
    df_pivot_idx = df_pivot_idx.sort_values("hourutc", ascending=True)
    df_pivot_idx['date'] = df_pivot_idx["hourutc"].dt.strftime('%Y-%m-%d')

    
    # ---- Animating ----

    # step 1: find maximum for each country
    # step 1a: maximum over positive, or absolute value then max

    abs_max_vals = df_pivot[['exchangege_mwh',
                   'exchangenl_mwh',
                   'exchangeno_mwh',
                   'exchangese_mwh',]].abs().max().to_dict()

    pie_frames = {'exchangege_mwh': [], 
              'exchangenl_mwh': [], 
              'exchangeno_mwh': [], 
              'exchangese_mwh': [],}

    map_frames = {}

    green = "#7BB662"
    yellow = "#FFD301"
    red = "#E03C32"
    central_point = (55.6761, 12.5683)

    # step 2:
    for column in ['exchangege_mwh', 'exchangenl_mwh', 
                   'exchangeno_mwh', 'exchangese_mwh',]:

        temp_df = df_pivot_idx.query(f"variable == '{column}'")
        max_val = abs_max_vals[column]

        for row in temp_df.itertuples():
            date = row.date
            value = row.exchange_mwh

            if value >= 0:
                values = [value, max_val - value]
                colors = [green, "#fdf0d5"]
                labels = ["Net import", "Remaining capacity"]
            else:
                values = [abs(value), max_val - abs(value)]
                colors = [red, "#fdf0d5"]
                labels = ["Net export", "Remaining capacity"]

            proportion = abs(value) / max_val

            if proportion > 0.95:
                map_color = red
            elif proportion > 0.8:
                map_color = yellow
            else:
                map_color = green

            # Pie frame data
            pie_frame = go.Frame(
                data = [go.Pie(
                    values=values,
                    labels=labels,
                    marker_colors=colors,
                    hole=0.2,
                    direction="clockwise",
                    sort=False,
                    hovertemplate="<extra></extra>"
                )],
                name=str(date)
            )
            pie_frames[column].append(pie_frame)

            if date not in map_frames:
                map_frames[date] = [] 

            # Add scatter point for this country
            scatter_point = go.Scattermapbox(
                lon=[row.longitude],
                lat=[row.latitude],
                mode='markers',
                marker=dict(
                    size=row.abs_exchange_mwh**(1/3),  
                    color=map_color,
                ),
                name=column,
                text=[value],
                showlegend=False,
                hovertemplate = "<b>Location</b><br>" + 
                    "Longitude: %{lon:.2f}<br>" + 
                    "Latitude: %{lat:.2f}<br>" + 
                    f"Exchange: %{value:.2f} MWh<br>" +
                    "<extra></extra>"
            )
            
            # Add line to central point
            line_trace = go.Scattermapbox(
                mode='lines',
                lon=[row.longitude, central_point[1]],
                lat=[row.latitude, central_point[0]],
                line=dict(
                    width=2,
                    color=map_color,
                ),
                name=f'Line {column}',
                showlegend=False,
            )
            
            map_frames[date].extend([scatter_point, line_trace])

    # Convert map_frames dictionary to list of frames
    map_frames_list = [
        go.Frame(
            data=traces,
            name=date
        )
        for date, traces in map_frames.items()
    ]

    # ---- Creating figure ----

    fig = make_subplots(
        rows=2, cols=3,
        specs=[[{"type": "scattermapbox", "rowspan": 2}, {"type": "pie"}, {"type": "pie"}],
               [None, {"type": "pie"}, {"type": "pie"}]],
        subplot_titles=("", "Germany", "Netherlands", "Norway", "Sweden"),
        column_widths=[0.5, 0.25, 0.25]
    )

    # Add initial traces
    # Initial map traces
    fig.add_traces(map_frames_list[0].data, rows=[1]*len(map_frames_list[0].data), 
                   cols=[1]*len(map_frames_list[0].data))

    # Initial pie traces
    positions = {
        'exchangege_mwh': (1, 2),
        'exchangenl_mwh': (1, 3),
        'exchangeno_mwh': (2, 2),
        'exchangese_mwh': (2, 3)
    }

    for column, (row, col) in positions.items():
        fig.add_trace(pie_frames[column][0].data[0], row=row, col=col)

    # Get the number of initial map traces
    n_map_traces = len(map_frames_list[0].data)

    # Create frames
    combined_frames = []
    frame_dates = sorted(map_frames.keys())

    for date in frame_dates:
        date_str = str(date)
        
        # Create frame for map
        map_data = map_frames[date]
        
        # Create frame for pies
        pie_data = []
        for column in positions.keys():
            matching_pie = next((frame for frame in pie_frames[column] 
                               if frame.name == date_str), None)
            if matching_pie:
                pie_data.append(matching_pie.data[0])
        
        # Combine all data
        frame = go.Frame(
            data=map_data + pie_data,  # Concatenate map and pie data
            name=date_str,
            traces=list(range(n_map_traces + len(positions)))  # Total number of traces
        )
        combined_frames.append(frame)

    # Set frames
    fig.frames = combined_frames

    # Update layout
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=56, lon=10),  # Center of your map
            zoom=4
        ),
        height=800,  # Adjust as needed
        showlegend=True,
        updatemenus=[{
            "buttons": [
                {
                    "args": [None, {"frame": {"duration": 500, "redraw": True},
                                  "fromcurrent": True}],
                    "label": "Play",
                    "method": "animate"
                },
                {
                    "args": [[None], {"frame": {"duration": 0, "redraw": True},
                                    "mode": "immediate",
                                    "transition": {"duration": 0}}],
                    "label": "Pause",
                    "method": "animate"
                }
            ],
            "direction": "left",
            "pad": {"r": 10, "t": 87},
            "showactive": False,
            "type": "buttons",
            "x": 0.1,
            "xanchor": "right",
            "y": 0,
            "yanchor": "top"
        }],
        sliders=[{
            "currentvalue": {
                "font": {"size": 16},
                "prefix": "Time: ",
                "visible": True,
                "xanchor": "right"
            },
            "steps": [
                {
                    "args": [[f.name], {
                        "frame": {"duration": 0, "redraw": True},
                        "mode": "immediate",
                        "transition": {"duration": 0}
                    }],
                    "label": f.name,
                    "method": "animate"
                }
                for f in combined_frames
            ]
        }]
    )

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
        "pricearea"
            ] 

    col_names = df.columns.tolist()
    columns = {str(k): str(k).capitalize() for k in
        list(set(col_names)-set(ignore_cols))}
 
    state["columns"] = columns 
    return columns

def handle_municipality(state, payload):
    print(f"Changing to municipality to {state["muni_JSON"][payload]}")
    state["main_page"]["municipalityno"] = int(payload)
    _update_main_plot(state) 

def handle_data_source(state, payload):
    state = state
    prev_source = state["main_page"]["data_source"]
    print(f"Changing from source {prev_source} to {payload}")
    state["data"][prev_source] = None
    state["main_page"]["data_source"] = payload 

    match payload:
        case "df_prodcons":
            _create_pie_map(state)
        case _:

            match payload:
                case "df_production":
                    state["data"]["df_production"]= _load_data("../data/production.csv") 
                case "df_consumption":
                    state["data"]["df_consumption"]= _load_data("../data/consumption.csv") 
                case "df_weather":
                    state["data"]["df_weather"]= _load_data("../data/weather.csv") 

            columns = _get_columns(state)
            state["main_page"]["data_column"] = (list(columns.values())[0]).lower()
            _update_main_plot(state)

    state["main_page"]["data_source"] = payload
    
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

def _pivot_data(state):
    # Current DataFrame in use
    current_source = state["main_page"]["data_source"]

    # What column we want to use
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
    # state["main_page"]["data_column"] = state["columns"].keys()[0]
    dict_mun = {f"{str(k)}": v for k, v in zip(muni_df['LAU-1 code 1'],
                                               muni_df['Municipality'])} 
    state["muni_JSON"] = dict_mun
    


## ---- Correlation ----

def handle_correlation(state):
    print("Changing to page Correlation")
    state["correlation"]["data"] = _load_correlation_data(state)
    _create_corr_fig(state)
    
def handle_dct_filter(state, payload):
    if eval(payload):
        state["correlation"]["dct_filter"] = 1
    else:
        state["correlation"]["dct_filter"] = 0
    _create_corr_fig(state)

def handle_dct_cutoff(state, payload):
    state["correlation"]["dct_cutoff"] = int(payload)
    _create_corr_fig(state)

def handle_lag(state, payload):
    state["correlation"]["lag"] = int(payload)
    _create_corr_fig(state)

def _load_correlation_data(state):
    # Sync Gas data and weather data
    df_gas = state["data"]["df_gas"]
    df_gas = df_gas[["GasDay", "PurchasePriceDKK_kWh"]]

    df_weather = state["data"]["df_weather"]
    df_wspd = df_weather[["hourutc", "wspd"]]
    df_wspd.set_index("hourutc", inplace=True)
    df_wspd = df_wspd[["wspd"]].resample("D").mean()
    df_wspd.reset_index(inplace=True)
    df_wspd = df_wspd[
        (df_wspd["hourutc"] >= min(df_gas["GasDay"])) &\
        (df_wspd["hourutc"] <= max(df_gas["GasDay"]))]
    df_wspd.reset_index(drop=True, inplace=True)

    return df_wspd

def _dct_swc(
    ts_prices: pd.Series,
    ts_avg_wspd: pd.Series,
    swc_width=7,
    lag=0,
    dct_cutoff=0,
    dct_filter=False
):
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


def _create_corr_fig(state):
    lag = state["correlation"]["lag"]
    swc_width = state["correlation"]["swc_width"]
    dct_filter = state["correlation"]["dct_filter"]
    dct_cutoff = state["correlation"]["dct_cutoff"]

    df = state["correlation"]["data"]
    df_gas = state["data"]["df_gas"]
    df_gas = df_gas.sort_values("GasDay", ascending=True)

    price_wspd, lag_avg_wspd = _dct_swc(
        df_gas["PurchasePriceDKK_kWh"],
        df["wspd"],
        swc_width=swc_width,
        lag=lag,
        dct_cutoff=dct_cutoff,
        dct_filter=dct_filter,
    )

    # Create figure with secondary y-axes
    fig = make_subplots(rows=2, cols=1, 
                        specs=[[{"secondary_y": True}],
                              [{"secondary_y": False}]])

    # Add traces for top plot
    # Purchase price on primary y-axis
    fig.add_trace(
        go.Scatter(
            x=df["hourutc"],
            y=df_gas["PurchasePriceDKK_kWh"],
            name="Purchase Price",
            line=dict(color="blue")
        ),
        row=1, col=1, secondary_y=False
    )

    # Wind speed on secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=df["hourutc"],
            y=lag_avg_wspd,
            name="Lagged Avg Wind Speed",
            line=dict(color="red")
        ),
        row=1, col=1, secondary_y=True
    )

    # Add trace for bottom plot (SWC)
    fig.add_trace(
        go.Scatter(
            x=df["hourutc"],
            y=price_wspd,
            name="SWC",
            showlegend=False,
            line=dict(color="blue")
        ),
        row=2, col=1
    )

    # Update layout
    fig.update_layout(
        height=700,  # Similar to matplotlib's figsize=(10, 7)
        title_text=f"Sliding window correlation (window = {swc_width}, lag = {lag})",
        showlegend=True
    )

    # Update axes labels
    fig.update_yaxes(title_text="Purchase price (DKK)", color="blue", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="(Lagged) avg. wind speed", color="red", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="SWC", row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=1)

    # Show figure
    state["correlation"]["fig"] = fig


## ---- Summary ----

def handle_summary(state):
    print("Changing to page Summary")
    if state["data"]["df_summary"] is None:
        temp_df = _load_data("../data/consumption.csv")
        muni_df = state["data"]["df_municipality"]
        merge_df = temp_df.merge(muni_df[["LAU-1 code 1", "Municipality"]],
                                 left_on="municipalityno", 
                                 right_on="LAU-1 code 1", how="left")
        merge_df = merge_df.drop("LAU-1 code 1", axis=1)
        merge_df = merge_df.rename(columns={"Municipality": "municipality"})
        print("merged!")

        state["data"]["df_summary"] = merge_df
    _consumption_group_pivot(state)
    _grouped_barplot(state)
 
def _consumption_group_pivot(state):
    response = "consumptionkwh"
    group_col = state["summary"]["group_col"]
    agg_method = state["summary"]["agg_method"]
    df = state["data"]["df_summary"]

    print(df.columns.tolist())

    print("#cecksdfj")
    df_agg = df.pivot_table(
        index=group_col, values=response, aggfunc=agg_method
    ).reset_index()
    df_agg = df_agg.sort_values(response, ascending=False) 

    agg_response = agg_method + "_" + response
    df_agg = df_agg.rename(columns={response: agg_response})

    bar_fig = px.bar(
        df_agg,
        group_col,
        agg_response,
    )

    state["summary"]["bar_fig"] = bar_fig


def _grouped_barplot(state):
    df = state["data"]["df_summary"]
    main_group = state["summary"]["main_group"]
    sub_group = state["summary"]["sub_group"]

    match main_group:
        case "municipality":
            sub_group = "branche"
        case "branche":
            sub_group = "municipality"

    hist_fig = px.histogram(
        df, x=main_group, y="consumptionkwh", barmode="group", color=sub_group,
    )
    state["summary"]["hist_fig"] = hist_fig

def handle_grouped_barplot(state, payload):
    state["summary"]["main_group"] = payload
    _grouped_barplot(state)


## ---- Forecasting ----

def handle_forecasting(state):
    print("Changing page to forecasting")
    _load_forecasting_data(state)
    _sarimax_plot(state)

def _load_forecasting_data(state):
    if state["data"]["df_production"] is None:
        temp_df = _load_data("../data/production.csv")
        muni_df = state["data"]["df_municipality"]
        merge_df = temp_df.merge(muni_df[["LAU-1 code 1", "Municipality"]],
                                 left_on="municipalityno", 
                                 right_on="LAU-1 code 1", how="left")
        merge_df = merge_df.drop("LAU-1 code 1", axis=1)
        merge_df = merge_df.rename(columns={"Municipality": "municipality"})
        print("merged!")

        state["data"]["df_forecasting"] = merge_df

def _sarimax_plot(state):
    plt.style.use("ggplot")

    df_prod = state["data"]["df_forecasting"]
    df_temp_wspd = state["data"]["df_weather"]
    prod_type = state["forecasting"]["prod_type"]
    cutoff = state["forecasting"]["cutoff"]
    municipality = state["forecasting"]["municipality"]
    include_temp = state["forecasting"]["include_temp"]
    include_wspd = state["forecasting"]["include_wspd"]
    
    df_prod = df_prod.query(f"municipality == '{municipality}'")
    df_temp_wspd = df_temp_wspd.query(f"municipality == '{municipality}'")
    df_prod["hourutc"]  = pd.to_datetime(df_prod["hourutc"])
    df_temp_wspd["hourutc"]  = pd.to_datetime(df_temp_wspd["hourutc"])
    df_prod.set_index("hourutc", inplace=True)
    df_temp_wspd.set_index("hourutc", inplace=True)

    endog = df_prod[prod_type]
    exog = df_temp_wspd[["temp", "wspd"]]

    train_data = endog[:cutoff]
    train_exog = exog[:cutoff]
    test_exog = exog[cutoff:]
    
    print("training model")
    mod = sm.tsa.statespace.SARIMAX(endog=train_data,
                                    exog=train_exog,
                                    trend="c", order=(1,1,1),
                                    seasonal_order=(1,0,1,24))

    print("training finished")        

    fit_mod = mod.fit(disp=False)
    res = mod.filter(fit_mod.params)

    pred_start = pd.to_datetime(cutoff) - pd.DateOffset(days=1)

    predict_step = res.get_forecast(steps=df_prod[cutoff:].shape[0],
                                    exog=test_exog[["temp", "wspd"]])

    fig = go.Figure()

    # Plot the actual observed values
    plot_start = pd.to_datetime(cutoff) - pd.DateOffset(months=2)
    fig.add_trace(
        go.Scatter(
            x=df_prod[plot_start:].index,
            y=df_prod[plot_start:][prod_type],
            mode='lines',
            name='Observed',
            line=dict(
                color='red',
                # size=3
            )
        )
    )

    # Ensure forecast index matches the test data index
    forecast_mean = predict_step.predicted_mean
    forecast_mean.index = df_prod[cutoff:].index
    forecast_ci = predict_step.conf_int()
    forecast_ci.index = df_prod[cutoff:].index

    # Plot the forecasted values
    fig.add_trace(
    go.Scatter(
        x=forecast_mean.index,
        y=forecast_mean,
        mode='lines',
        line=dict(dash='dash', color='blue'),
        name='Forecast'
    )
    )


    # # Add confidence intervals
    # fig.add_trace(
    # go.Scatter(
    # x=forecast_ci.index,
    # y=forecast_ci.iloc[:, 0],
    # fill=None,
    # mode='lines',
    # line=dict(width=0),
    # showlegend=False
    # )
    # )
    #
    # fig.add_trace(
    # go.Scatter(
    # x=forecast_ci.index,
    # y=forecast_ci.iloc[:, 1],
    # fill='tonexty',
    # mode='lines',
    # line=dict(width=0),
    # fillcolor='rgba(0, 0, 255, 0.1)',
    # name='95% Confidence Interval'
    # )
    # )

    # Update layout
    fig.update_layout(
    title='Solar Power Production Forecast for Copenhagen',
    xaxis_title='Date',
    yaxis_title='Solar Power Production (MWh)',
    showlegend=True,
    template='plotly_white',
    hovermode='x unified',
    width=1000,
    height=600
    )

    # Add gridlines
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')


    state["forecasting"]["fig"] = fig


    
## ---- Initializations ---- 

database = _mongodb_connect()

# State object is a dictionary.
initial_state = wf.init_state(
    {
        # DataFrames - insert into "data": {} ? 
        "data": {
            # # Cassandra tables in .csv format
            "df_production": None,
            "df_prodcons": None,
            "df_weather": _load_data("../data/weather.csv"),
            "df_consumption": None,

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
            "title": "Dashboard of Energy Production in Denmark"
        },

        "correlation": {
            "data": None,
            "lag": 7,
            "dct_filter": 1,
            "dct_cutoff": 5,
            "swc_width": 7,
        },

        "forecasting": {
            "cutoff": "2022-09-01",
            "municipality": "Copenhagen",
            "prod_type": "solarmwh",
            "incl_temp": 0,
            "incl_wspd": 0,
        },

        "summary": {
            "data": None,
            "group_col": "municipality",
            "agg_method": "mean",
            "plot": 1,

            "main_group": "branche",
            "sub_group": "municipality"
        },

        "columns" : None,
    }
)

_pivot_data(initial_state)
_get_municipality_JSON(initial_state)

# Main page initializations
_create_map(initial_state)
_create_line_plot(initial_state)
_create_main_plot(initial_state)
_get_columns(initial_state)


