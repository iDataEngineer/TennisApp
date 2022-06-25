# Tennis App
import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)

def run():
    st.set_page_config(
        page_title="Home",
        page_icon="🏛",
    )


    page_names_to_funcs = {
        "Home": 'app',
        "ATP Tour Stats": 'ATP_tour',
        "Grand Slam Predictions": 'Slam_ML',
    }

    selected_page = st.sidebar.selectbox("Select a page", page_names_to_funcs.keys())


    st.title('Tennis App')
    st.markdown('---')

    st.sidebar.success("Select a demo above.")

    st.markdown(
        """
            This is the text bit...    
        """
    )


if __name__ == "__main__":
    run()

