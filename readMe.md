<h1 align="center"><b> 🥇 Tennis App 🥇 </b></h1>
<br/>

An application with a Streamlit front-end and a back-end database, updated daily via Github actions.

- The last completed tournament is shown on the side bar.

- The side bar allows users to filter the data on player, year, surface and tour level.

- The app has data from the start of the ATP tour (1990)
  - There are some gaps from the earlier days.

---
<br/>

### 🧱 **Build Status**

| | <s></s>
|--|--
| Data Pipeline | [![Data Pipeline](https://github.com/iDataEngineer/TennisApp/actions/workflows/data_pipeline.yml/badge.svg)](https://github.com/iDataEngineer/TennisApp/actions/workflows/data_pipeline.yml)
| Streamlit App | [![Streamlit App](https://img.shields.io/badge/Streamlit-TennisApp-brightgreen?icon=github)](https://tennis.streamlit.app/)

---
<br/>

### 🐳 Docker

Build the image

````
docker build -t streamlit_img .
````

Run the container

````
docker run --name streamlit_container -p 8501:8501 streamlit_img:latest
````
