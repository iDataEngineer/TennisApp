import requests, PIL, streamlit as st
from io import BytesIO

# Get image from 
@st.cache(show_spinner=False)
def get_header(source = 'https://raw.githubusercontent.com/iDataEngineer/ATP-SlamApp/main/data/SlamApp_BG.jpg', new_width = 1200):
    # if from web
    if source[:4] == 'http':
        source = requests.get(source).content

        # convert to PIL suitable format
        source = BytesIO(source)    
        
    # format image
    img = PIL.Image.open(source)
    wpercent = (new_width/float(img.size[0]))
    hsize = int((float(img.size[1])*float(wpercent)))
    
    return img.resize((new_width,hsize), PIL.Image.ANTIALIAS)

if __name__ == '__main__':
    test = get_header(new_width=800, source='SlamApp_BG.jpg')
