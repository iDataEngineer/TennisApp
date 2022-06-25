# Tennis App
import streamlit as st, PIL, requests
from io import BytesIO
from streamlit.logger import get_logger

LOGGER = get_logger(__name__.title())

def run():
    st.set_page_config(layout="wide")

    # Homepage items
    st.title('Welome to the Tennis App...')
    st.markdown('''---''')

    st.image(get_header())

@st.cache(show_spinner=False)
def get_header(url = 'https://raw.githubusercontent.com/iDataEngineer/ATP-SlamApp/main/data/SlamApp_BG.jpg', new_width = 1200):
    # get from web
    req = requests.get(url).content
    image_req = BytesIO(req)    
    
    # format image
    img = PIL.Image.open(image_req)
    wpercent = (new_width/float(img.size[0]))
    hsize = int((float(img.size[1])*float(wpercent)))
    
    return img.resize((new_width,hsize), PIL.Image.ANTIALIAS)

if __name__ == "__main__":
    run()