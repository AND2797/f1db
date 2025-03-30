import pandas as pd
import streamlit as st
import fastf1 as f1
from StreamlitUI.src.F1DataLoaderRest.client import F1DataLoaderClient
from StreamlitUI.src.AppUtils.config_utils import get_property
from StreamlitUI.src.AppUtils.logger_utils import setup_logger

logger = setup_logger(get_property("App", "log_file"))


def load_data(request_df: pd.DataFrame):
    if request_df.empty:
        return
    request_df.drop_duplicates(inplace=True)
    request_df['Year'] = request_df['Year'].astype(int)
    host = get_property("App", "data_loader_server")
    data_loader_client = F1DataLoaderClient(f"{host}")
    request_df.apply(lambda x: data_loader_client.load_session_data(x['Year'],
                                                                    x['Race'],
                                                                    x['Session']), axis=1)


def main():
    st.title("Dataloader")
    year = st.text_input("Year")
    if year:
        logger.info("Fetching event schedule")
        #TODO: this getting called everytime user interacts with the editable dataframe. Fix this
        events = f1.get_event_schedule(int(year))
        events = events[['Country', 'EventName', 'OfficialEventName', 'EventDate']]
        events = events.rename({'EventName': 'Event Name', 'OfficialEventName': 'Official Name', 'EventDate': 'Event Date'})
        st.session_state[f"events_{year}"] = events
        events.loc[:, "Load"] = False
        events.loc[:, "Session"] = ""
        if year and f"events_{year}" in st.session_state:
            selected = st.data_editor(events,
                                      disabled=('Country', 'EventName', 'OfficialEventName'),
                                      hide_index=True)

            if st.button("Load Data"):
                selected = selected.loc[selected["Load"]]

                # Create request_df for loading data
                selected.loc[:, "Session"] = selected["Session"].apply(lambda x: [item.lower().strip() for item in x.split(',')])
                request_df = pd.DataFrame(columns=["Year", "Race", "Session"])
                request_df["Year"] = selected["EventDate"].dt.year
                request_df["Race"] = selected["EventName"].str.replace(' ', '_').str.replace('-', '_')
                request_df["Session"] = selected["Session"]
                request_df = request_df.explode(["Session"])
                load_data(request_df)


if __name__ == '__main__':
    main()