import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from streamlit_service.src.Arrow.arrow_client import arrow_duckdb_client
from streamlit_service.src.Analytics.driver_consistency_plots import make_consistency_plot, get_lap_query
from streamlit_service.src.Analytics.telemetry_plots import plot_laps, get_telemetry_query, plot_telemetry


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
    # available_tables = get_available_tables()
    possible_filters_query = f"SELECT DISTINCT year, race, session, driver from laps"
    possible_filters = arrow_duckdb_client.execute(possible_filters_query)

    if 'option_year' not in st.session_state:
        st.session_state['option_year'] = []
    if 'option_race' not in st.session_state:
        st.session_state['option_race'] = []
    if 'option_session' not in st.session_state:
        st.session_state['option_session'] = []
    if 'option_driver' not in st.session_state:
        st.session_state['option_driver'] = []

    #TODO: implement these cascading filters better. clean up
    # Select year
    year = set(possible_filters['year'].tolist())
    option_year = st.multiselect("Year", year, default=st.session_state['option_year'])

    st.session_state['option_year'] = option_year
    if option_year:
        filter_on_year = possible_filters['year'].isin(option_year)
        race = set(possible_filters[filter_on_year]['race'].tolist())
        option_race = st.multiselect("Race", race, default=st.session_state['option_race'])
        st.session_state['option_race'].extend(option_race)
        if option_race:
            filter_on_race = possible_filters['race'].isin(option_race)
            session = set(possible_filters[filter_on_race & filter_on_year]['session'].tolist())
            option_session = st.multiselect("Session", session, default=st.session_state['option_session'])
            st.session_state['option_session'] = option_session
            if option_session:
                filter_on_session = possible_filters['session'].isin(option_session)
                driver = set(possible_filters[filter_on_session & filter_on_year & filter_on_race]['driver'].tolist())
                option_driver = st.multiselect("Driver", driver, default=st.session_state['option_driver'])
                st.session_state['option_driver'] = option_driver
                if option_driver:
                    st.write(option_driver[0])
                    # driver_list = ", ".join(option_driver)
                    # lap_data_query = f"""
                    #                     SELECT driver, lap_number, lap_time
                    #                     FROM laps
                    #                     WHERE year IN ({year_list}) AND
                    #                     race in ({race_list}) AND
                    #                     session IN ({session_list}) AND
                    #                     driver IN ({driver_list})
                    #                 """
                    # data = arrow_duckdb_client.execute(lap_data_query)
                    # st.dataframe(data)



    # Fetch and store distinct drivers for the selected dataset
    # driver_query = f"-- SELECT distinct(driver) FROM {option}.laps"
    # driver_list = arrow_duckdb_client.execute(driver_query)['driver'].tolist()

    # # Store driver_list in session state if not already there, or if the dataset changes
    # if 'driver_list' not in st.session_state or st.session_state.get('current_dataset') != option:
    #     st.session_state['driver_list'] = driver_list
    #     st.session_state['current_dataset'] = option
    #
    # driver_consistency, telemetry = st.tabs(["Driver Consistency", "Telemetry"])
    # with driver_consistency:
    #     # Multiselect for Drivers
    #     drivers = st.multiselect("Select Drivers", st.session_state['driver_list'])
    #
    #     # year, session, race, driver
    #
    #     lap_data_query = get_lap_query(option, drivers)
    #     try:
    #         full_df_laps = arrow_duckdb_client.execute(lap_data_query)
    #         fig = make_consistency_plot(full_df_laps)
    #
    #         # Display the plot in Streamlit
    #         st.plotly_chart(fig, use_container_width=True)
    #     except Exception as e:
    #         st.error(f"Error fetching data: {e}")
    #         st.error("Please ensure the selected dataset and drivers are valid and accessible.")
    #     else:
    #         st.info("Please select at least one driver to display the violin plot.")
    #
    # with telemetry:
    #     # Multiselect for Drivers
    #     drivers = st.multiselect("Select Drivers Telemetry", st.session_state['driver_list'])
    #
    #     lap_data_query = get_lap_query(option, drivers)
    #     try:
    #         df_laps = arrow_duckdb_client.execute(lap_data_query)
    #         fig = plot_laps(df_laps)
    #         # fig = make_telemetry_plot(df)
    #         points = st.plotly_chart(fig, use_container_width=True, selection_mode="points", on_select='rerun')
    #         points_to_plot = [[record['x'], record['legendgroup']] for record in points['selection']['points']]
    #
    #         # driver = points['selection']['points'][0]['legendgroup']
    #         # lap_num = points['selection']['points'][0]['x']
    #         where_clauses = " OR ".join(
    #             [f"(driver = '{driver}' AND lap_number = {lap})" for lap, driver in points_to_plot]
    #         )
    #         query = get_telemetry_query(option, where_clauses)
    #         df_tele = arrow_duckdb_client.execute(query)
    #         fig = plot_telemetry(df_tele)
    #         st.plotly_chart(fig)
    #     except Exception as e:
    #         st.error(f"Error fetching data: {e}")
    #         st.error("Please ensure the selected dataset and drivers are valid and accessible.")
    #     else:
    #         st.info("Please select at least one driver to display the violin plot.")

if __name__ == '__main__':
    main()
