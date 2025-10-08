import streamlit as st
import plotly.express as px


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


def make_consistency_plot(df):

    if df.empty:
        st.info(f"No lap data available")
        return

    min_lap = int(df['lap_number'].min())
    max_lap = int(df['lap_number'].max())

    lap_range = st.slider(
        "Select Lap Range",
        min_value=min_lap,
        max_value=max_lap,
        value=(min_lap, max_lap),
        step=1
    )
    
    selected_laps = df[
        (df['lap_number'] >= lap_range[0]) &
        (df['lap_number'] <= lap_range[1])
    ]    
    
    if selected_laps.empty:
        st.info(f"No lap data available within laps {lap_range[0]} - {lap_range[1]}")
        return
    
    fig = px.violin(
        selected_laps,
        x='driver',
        y='lap_time',
        color='driver',
        box=True,
        points='all',
        title=f"Driver Lap Time Consistency (Laps {lap_range[0]} - {lap_range[1]})",
        labels={"lap_time": "Lap Time (seconds)", "driver": "Driver"}
    )
    
    
    fig.update_layout(
        xaxis_title='Driver',
        yaxis_title='Lap Time',
        hovermode='closest',
        margin=dict(l=40, r=40, t=40, b=40)
    )
    
    return fig
