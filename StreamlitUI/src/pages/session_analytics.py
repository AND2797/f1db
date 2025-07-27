import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import pandas as pd
from StreamlitUI.src.ArrowClient.arrow_client import arrow_duckdb_client
from streamlit_plotly_events import plotly_events


# box plot of driver lap times
# race, session, driver, laps

def get_available_tables():
    # TODO: make this a common function / utility?
    query = "SELECT * FROM information_schema.tables;"
    df = arrow_duckdb_client.execute(query)
    df = df.rename({'table_catalog': 'database', 'table_name': 'table'}, axis=1)
    df = df[['database', 'table']]
    df['race'] = df['database'].str.split('_')
    df['year'] = df['race'].str[-1]
    df['session'] = df['race'].str[-2]
    df['race'] = df['race'].str[0:3].str.join(' ').str.title()
    df['full_path'] = df['database'] + '.' + df['table']
    # filtered_df = df[df['full_path'].str.endswith('.laps')]
    return df


def main():
    # TODO: review this code
    st.title("Session Analytics")
    available_tables = get_available_tables()
    # Select year
    option_year = st.selectbox(
        "Year",
        available_tables['year'].drop_duplicates()
    )
    if option_year:
        filter_on_year = available_tables['year'] == option_year
        option_race = st.selectbox(
            "Race",
            available_tables.loc[filter_on_year]['race'].drop_duplicates()
        )
        if option_race:
            filter_on_race = available_tables['race'] == option_race
            option_session = st.selectbox(
                "Session",
                available_tables.loc[filter_on_race & filter_on_year].drop_duplicates()
            )
            if option_session:
                option = option_session





    # Fetch and store distinct drivers for the selected dataset
    driver_query = f"SELECT distinct(driver) FROM {option}.laps"
    driver_list = arrow_duckdb_client.execute(driver_query)['driver'].tolist()

    # Store driver_list in session state if not already there, or if the dataset changes
    if 'driver_list' not in st.session_state or st.session_state.get('current_dataset') != option:
        st.session_state['driver_list'] = driver_list
        st.session_state['current_dataset'] = option

    driver_consistency, telemetry = st.tabs(["Driver Consistency", "Telemetry"])
    with driver_consistency:
        # Multiselect for Drivers
        selected_drivers = st.multiselect("Select Drivers", st.session_state['driver_list'])

        df_laps = None # Initialize df_laps outside the conditional block

        if selected_drivers:
            # Fetch the entire lap data for selected drivers first to determine min/max laps
            drivers_tuple = tuple(selected_drivers)
            if len(selected_drivers) == 1:
                query_drivers = f"('{selected_drivers[0]}')"
            else:
                query_drivers = str(drivers_tuple)

            # Query all laps for selected drivers to determine min/max for the slider
            initial_data_query = f"""
            SELECT driver, lap_number, lap_time
            FROM {option}.laps
            WHERE driver IN {query_drivers}
            """

            try:
                full_df_laps = arrow_duckdb_client.execute(initial_data_query)

                if not full_df_laps.empty:
                    # Determine min and max lap numbers for the slider
                    min_lap = int(full_df_laps['lap_number'].min())
                    max_lap = int(full_df_laps['lap_number'].max())

                    # Create a slider for lap range
                    lap_range = st.slider(
                        "Select Lap Range",
                        min_value=min_lap,
                        max_value=max_lap,
                        value=(min_lap, max_lap), # Default to the full range
                        step=1
                    )

                    # Filter the DataFrame based on the selected lap range
                    df_laps = full_df_laps[
                        (full_df_laps['lap_number'] >= lap_range[0]) &
                        (full_df_laps['lap_number'] <= lap_range[1])
                    ]

                    if not df_laps.empty:
                        # Create the violin plot using Plotly Express
                        fig = px.violin(
                            df_laps,
                            x="driver",
                            y="lap_time",
                            color="driver",
                            box=True,  # Optionally show a box plot inside the violin
                            points="all", # Optionally show all points
                            title=f"Driver Lap Time Consistency (Laps {lap_range[0]} - {lap_range[1]})",
                            labels={"lap_time": "Lap Time (seconds)", "driver": "Driver"}
                        )

                        # Update layout for better readability
                        fig.update_layout(
                            xaxis_title="Driver",
                            yaxis_title="Lap Time",
                            hovermode="closest",
                            margin=dict(l=40, r=40, t=40, b=40)
                        )

                        # Display the plot in Streamlit
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(f"No lap data available for the selected drivers within laps {lap_range[0]} - {lap_range[1]}.")
                else:
                    st.info("No lap data available for the selected drivers in this dataset.")
            except Exception as e:
                st.error(f"Error fetching data: {e}")
                st.error("Please ensure the selected dataset and drivers are valid and accessible.")
        else:
            st.info("Please select at least one driver to display the violin plot.")

    with telemetry:
        # Multiselect for Drivers
        selected_drivers = st.multiselect("Select Drivers 2", st.session_state['driver_list'])

        df_laps = None # Initialize df_laps outside the conditional block

        if selected_drivers:
            # Fetch the entire lap data for selected drivers first to determine min/max laps
            drivers_tuple = tuple(selected_drivers)
            if len(selected_drivers) == 1:
                query_drivers = f"('{selected_drivers[0]}')"
            else:
                query_drivers = str(drivers_tuple)

            # Query all laps for selected drivers to determine min/max for the slider
            initial_data_query = f"""
            SELECT driver, lap_number, lap_time
            FROM {option}.laps
            WHERE driver IN {query_drivers}
            """

            try:
                full_df_laps = arrow_duckdb_client.execute(initial_data_query)
                fig = px.scatter(full_df_laps, 'lap_number', 'lap_time', color='driver')
                # Update layout for better readability
                fig.update_layout(
                    xaxis_title="Lap Number",
                    yaxis_title="Lap Time (s)",
                    hovermode="closest",
                    margin=dict(l=40, r=40, t=40, b=40)
                )
                fig.update_traces(marker=dict(size=15))

                # Display the plot in Streamlit
                points = st.plotly_chart(fig, use_container_width=True, selection_mode="points", on_select='rerun')

                points_to_plot = [[record['x'], record['legendgroup']] for record in points['selection']['points']]
                where_clauses = " OR ".join(
                    [f"(driver = '{driver}' AND lap_number = {lap})" for lap,driver in points_to_plot]
                )
                # plot telemetry based on selection
                driver = points['selection']['points'][0]['legendgroup']
                lap_num = points['selection']['points'][0]['x']
                telemetry_query = f"""
                SELECT * FROM {option}.telemetry
                WHERE {where_clauses}
                """
                tele_df = arrow_duckdb_client.execute(telemetry_query)
                print(tele_df.columns)
                # TODO: make it work for multiple drivers
                # TODO: interpolate distance
                fig = make_subplots(rows=4, cols=1, shared_xaxes=True)
                fig.add_trace(go.Scatter(x=tele_df['distance'], y=tele_df['speed'], mode='lines'), row=1, col=1)
                fig.add_trace(go.Scatter(x=tele_df['distance'], y=tele_df['throttle'], mode='lines'), row=2, col=1)
                fig.add_trace(go.Scatter(x=tele_df['distance'], y=tele_df['rpm'], mode='lines'), row=3, col=1)
                fig.add_trace(go.Scatter(x=tele_df['distance'], y=tele_df['brake'], mode='lines'), row=4, col=1)
                fig.update_layout(height=900, showlegend=False)
                fig.update_xaxes(showgrid=True)
                fig.update_yaxes(showgrid=True)
                # Update layout for better readability
                # fig.update_layout(
                #     xaxis_title="Distance",
                #     yaxis_title="Speed",
                #     hovermode="closest",
                #     margin=dict(l=40, r=40, t=40, b=40)
                # )
                st.plotly_chart(fig)

            except Exception as e:
                pass


if __name__ == '__main__':
    # Ensure arrow_duckdb_client is initialized or accessible
    main()