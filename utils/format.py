import streamlit as st

# Wide Mode 
st.cache(show_spinner=False)
def auto_wide_mode():
    st.set_page_config(layout="wide")