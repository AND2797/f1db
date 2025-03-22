import pandas as pd
import streamlit as st
from StreamlitUI.F1DataLoaderRest.client import F1DataLoaderClient


def load_data(request_df: pd.DataFrame):
    if request_df.empty:
        return
    request_df.drop_duplicates(inplace=True)
    request_df['Year'] = request_df['Year'].astype(int)
    data_loader_client = F1DataLoaderClient("http://localhost:8000")
    request_df.apply(lambda x: data_loader_client.load_session_data(x['Year'],
                                                                    x['Race'],
                                                                    x['Session']), axis=1)


def main():
    st.title("Dataloader")
    data = pd.DataFrame(columns=["Year", "Race", "Session"])
    request = st.data_editor(data, num_rows="dynamic", use_container_width=True)
    if st.button("Load"):
        load_data(request)


if __name__ == '__main__':
    main()