<h2 align="center">dropshot</h2>

<h3 align="center">Tennis Prediction Modelling</h3>

---

| <!--  --> | <!--  -->
|--|--
| **Licence** | [MIT Licence](/License)
| **Status** | Developing first release
| **CI Status** | [![Matches Pipeline](https://github.com/DNYFZR/TennisApp/actions/workflows/data_pipeline.yml/badge.svg)](https://github.com/DNYFZR/TennisApp/actions/workflows/data_pipeline.yml)
| **CD Status** | To be configured
<!-- | **App Link** | [![Streamlit App](https://img.shields.io/badge/Streamlit-TennisApp-brightgreen?icon=github)](https://tennis.streamlit.app/) -->

#### Scope - to be updated for new setup...

- Set up project repo and board (GitHub)

- Merge together 3 existing repos

- Consolodate the data pipelines

- Implement duckDB data & feature store

- Integreate tensorflow neural net build pipeline for ATP & WTA tour matches

- Develop a Streamlit App to allow users to access all data, stats & prediction outputs

- Allow users to model player vs. player combinations on different surfaces / tour levels etc.

<h3 align="center">Development</h3>

Development on this project is carried out using Python and SQL.

Please check the [contribution guide](/docs/CONTRIBUTE.md) for the most upto date information.

The main packages in use are :

- Streamlit - app development
- DuckDB - data storage, management & processing
- Tensorflow - model building

The main tools / services in use are :

- Streamlit Cloud - app deployment
- Tensorflow Hub - model serving
- Docker Desktop - app containerisation

#### Database

- This module manages the required interations with Duck DB for project data...

  - Create DB
  - Create table
  - Execute query

#### Pipelines

- This module extracts all the point by point and match data from the source repos, summarises for both players on ATP & WTA tours;

  - Set, game & point win stats - total counts

  - Serve stats; aces, 1st in, second in etc. - total counts

  - Serve speeds - max & average for each

  - Winners & unf errors - total

- This module builds the feature set required to build & train the SlamModel.

#### Models

- Build

  - The main function of this moduel is to build the deep learning model architecture.

- Feature Map

  - This module extracts the features for the live tournament from the source data.

- Predict

  - This module manages the prediction of matches with the SlamModel.

#### Docker

````ps1
# Build an image
docker build -t streamlit_img .

#Run the container
docker run --name streamlit_container -p 8501:8501 streamlit_img:latest

````
