import pandas as pd
import streamlit as st
from StreamlitUI.src.F1DataLoaderRest.client import F1DataLoaderClient
from StreamlitUI.src.AppUtils.config_utils import get_property


def load_data(request_df: pd.DataFrame):
    if request_df.empty:
        return
    request_df.drop_duplicates(inplace=True)
    request_df['Year'] = request_df['Year'].astype(int)
    host = get_property("App", "data_loader_service")
    data_loader_client = F1DataLoaderClient(f"{host}")
    request_df.apply(lambda x: data_loader_client.load_session_data(x['Year'],
                                                                    x['Race'],
                                                                    x['Session']), axis=1)


def main():
    st.title("Dataloader")
    data = pd.DataFrame(columns=["Year", "Race", "Session"])
    available_sessions = ["Race", "Qualifying", "Practice 1", "Practice 2", "Practice 3", "Sprint Qualifying", "Sprint"]
    request = st.data_editor(data, num_rows="dynamic", use_container_width=True,
                             column_config={"Session": st.column_config.SelectboxColumn(options=available_sessions,
                                                                                          required=True)})
    if st.button("Load"):
        request["Race"] = request["Race"].str.lower()
        request["Race"] = request["Race"].str.replace(' ', '_')
        load_data(request)


if __name__ == '__main__':
    main()