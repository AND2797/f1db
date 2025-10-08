import streamlit as st
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objs as go


def get_lap_query(table, drivers):
    drivers_tuple = tuple(drivers)
    if len(drivers_tuple) == 1:
        query_drivers = f"('{drivers[0]}')"
    else:
        query_drivers = str(drivers_tuple)

    # Query all laps for selected drivers to determine min/max for the slider
    # read data from xml
    lap_data_query = f"""
    SELECT driver, lap_number, lap_time
    FROM {table}.laps
    WHERE driver IN {query_drivers}
    """
    return lap_data_query


def plot_laps(df):
    fig = px.scatter(df, 'lap_number', 'lap_time', color='driver')
    # Update layout for better readability
    fig.update_layout(
        xaxis_title="Lap Number",
        yaxis_title="Lap Time (s)",
        hovermode="closest",
        margin=dict(l=40, r=40, t=40, b=40)
    )
    fig.update_traces(marker=dict(size=15))
    return fig


def get_telemetry_query(table, where_clause):
    telemetry_query = f"""
        SELECT * FROM {table}.telemetry
        WHERE {where_clause}
    """
    return telemetry_query


def plot_telemetry(df):
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True)
    fig.add_trace(go.Scatter(x=df['distance'], y=df['speed'], mode='lines'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['distance'], y=df['throttle'], mode='lines'), row=2, col=1)
    fig.add_trace(go.Scatter(x=df['distance'], y=df['rpm'], mode='lines'), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['distance'], y=df['brake'], mode='lines'), row=4, col=1)
    fig.update_layout(height=900, showlegend=False)
    fig.update_xaxes(showgrid=True)
    fig.update_yaxes(showgrid=True)
    return fig


