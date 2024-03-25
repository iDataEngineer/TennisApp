<h2 align="center"><b> 🏓 Pure Aero </b></h2>

<h3 align="center"><b> Tennis Stats & Match Prediction App </b></h3>

---

| <!--  --> | <!--  -->
|--|--
| **Licence** | [MIT Licence](../LICENSE)
| **Status** | Developing first release
| **Pipelines** | [![Matches Pipeline](https://github.com/DNYFZR/TennisApp/actions/workflows/data_pipeline.yml/badge.svg)](https://github.com/DNYFZR/TennisApp/actions/workflows/data_pipeline.yml)
| **App Link** | [![Streamlit App](https://img.shields.io/badge/Streamlit-TennisApp-brightgreen?icon=github)](https://tennis.streamlit.app/)

#### **Scope**

````markdown
- Set up project repo and board (GitHub)

- Merge together 3 existing repos

- Consolodate the data pipelines

- Implement duckDB data & feature store

- Integreate tensorflow neural net build pipeline for ATP & WTA tour matches

- Develop a Streamlit App to allow users to access all data, stats & prediction outputs

- Allow users to model player vs. player combinations on different surfaces / tour levels etc.

````

---

<h3 align="center"><b> 📝 Admin </b></h3>

Contributions : Please check the [contribution guide](/docs/CONTRIBUTE.md) for the most upto date information.

Security : Please check the [contribution guide](/docs/SECURITY.md) for the most upto date information.

---

<h3 align="center"><b> 🥞 Stack  </b></h3>

Development on this project is carried out using Python and SQL.

The main packages in use are :

- Streamlit - app development
- DuckDB - data storage, management & processing
- Tensorflow - model building

The main tools / services in use are :

- Streamlit Cloud - app deployment
- Tensorflow Hub - model serving
- Docker Desktop - app containerisation

For full details on packages used in development, please see the [dependencies directory](../dependencies/)

---

<h3 align="center"><b> 📊 App  </b></h3>

An application with a Streamlit front-end...


---

<h3 align="center"><b> 🧱 Prediction Model </b></h3>

#### **1. Database**

This module manages the required interations with Duck DB for project data...

- Create DB
- Create table
- Execute query

#### **2. Pipelines**

This module extracts all the point by point and match data from the source repos, summarises for both players on ATP & WTA tours;

- Set, game & point win stats - total counts

- Serve stats; aces, 1st in, second in etc. - total counts

- Serve speeds - max & average for each

- Winners & unf errors - total

This module builds the feature set required to build & train the SlamModel.

#### **3. Models**

Build

- The main function of this moduel is to build the deep learning model architecture.

Feature Map

- This module extracts the features for the live tournament from the source data.

Predict

- This module manages the prediction of matches with the SlamModel.

#### 🐳 Docker

Build an image

````ps1
docker build -t streamlit_img .
````

Run the container

````ps1
docker run --name streamlit_container -p 8501:8501 streamlit_img:latest
````

<br>

---
---
