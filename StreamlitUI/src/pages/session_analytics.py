import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from StreamlitUI.src.Arrow.arrow_client import arrow_duckdb_client
from StreamlitUI.src.Analytics.driver_consistency_plots import make_consistency_plot, get_lap_query
from StreamlitUI.src.Analytics.telemetry_plots import plot_laps, get_telemetry_query, plot_telemetry


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
        drivers = st.multiselect("Select Drivers", st.session_state['driver_list'])

        lap_data_query = get_lap_query(option, drivers)
        try:
            full_df_laps = arrow_duckdb_client.execute(lap_data_query)
            fig = make_consistency_plot(full_df_laps)

            # Display the plot in Streamlit
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            st.error("Please ensure the selected dataset and drivers are valid and accessible.")
        else:
            st.info("Please select at least one driver to display the violin plot.")

    with telemetry:
        # Multiselect for Drivers
        drivers = st.multiselect("Select Drivers Telemetry", st.session_state['driver_list'])

        lap_data_query = get_lap_query(option, drivers)
        try:
            df_laps = arrow_duckdb_client.execute(lap_data_query)
            fig = plot_laps(df_laps)
            # fig = make_telemetry_plot(df)
            points = st.plotly_chart(fig, use_container_width=True, selection_mode="points", on_select='rerun')
            points_to_plot = [[record['x'], record['legendgroup']] for record in points['selection']['points']]

            # driver = points['selection']['points'][0]['legendgroup']
            # lap_num = points['selection']['points'][0]['x']
            where_clauses = " OR ".join(
                [f"(driver = '{driver}' AND lap_number = {lap})" for lap, driver in points_to_plot]
            )
            query = get_telemetry_query(option, where_clauses)
            df_tele = arrow_duckdb_client.execute(query)
            fig = plot_telemetry(df_tele)
            st.plotly_chart(fig)
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            st.error("Please ensure the selected dataset and drivers are valid and accessible.")
        else:
            st.info("Please select at least one driver to display the violin plot.")

if __name__ == '__main__':
    main()
