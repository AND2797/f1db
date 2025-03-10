import pandas as pd
import streamlit as st
from src.data_processor.process_f1_data import write_session_data


def load_data(request_df: pd.DataFrame):
    if request_df.empty:
        return

    request_df.drop_duplicates(inplace=True)
    request_df['Year'] = request_df['Year'].astype(int)

    request_df.apply(lambda x: write_session_data(x['Year'], x['Race'], x['Session']), axis=1)


def main():
    st.title("Dataloader")
    data = pd.DataFrame(columns=["Year", "Race", "Session"])
    request = st.data_editor(data, num_rows="dynamic", use_container_width=True)
    if st.button("Load"):
        load_data(request)


if __name__ == '__main__':
    main()