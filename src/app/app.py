import re, datetime as dt, duckdb, pandas as pd, streamlit as st
# import keras, tensorflow_hub as hub

st.set_page_config(layout="wide")

@st.cache_data
def query_database(query:str):
  """Query the point-by-point feature store"""
  return duckdb.connect("data/database/point-by-point.db").execute(query).df()

@st.cache_data
def get_atp_matches():
  return pd.read_parquet("data/tables/ATP_matches.parquet").sort_values("tourney_date", ascending=False)

@st.cache_data
def get_wta_matches():
  return pd.read_parquet("data/tables/WTA_matches.parquet").sort_values("tourney_date", ascending=False)

@st.cache_data
def get_all_matches():
  return pd.concat([get_atp_matches(), get_wta_matches()], axis=0)

# @st.cache_data
# def predict(data:dict, path:str = None):
#   # Update the input format 
#   if path:
#     model = keras.Sequential([
#         hub.KerasLayer('model_assets_2023-01-04')
#     ])

#     data = pd.read_csv(path)

#     output = model(data.iloc[:,7:].copy().to_numpy())
#     data['p1_win_pred'] = output
#     data['p2_win_pred'] = 1 - output
    
#   return data

@st.cache_data
def players():
  """Get a list of players from get_all_matches"""
  return pd.concat([get_all_matches()['winner_name'], get_all_matches()['loser_name']], axis=0).sort_values(ascending=True).unique()

def controls():
  """Sidebar controls"""
  col1, col2, col3 = st.columns(3)
  
  selected_player = col1.selectbox(
    label="Select Player...",
    options=["All", *players()],
    index=list(players()).index("Carlos Alcaraz")+1,
  )

  selected_round = col2.number_input(
    label="Round",
    max_value=7,
    min_value=1,
    step=1,
    value=7,
  )

  selected_year = col3.number_input(
    label="Year",
    min_value=2011,
    max_value=dt.datetime.today().year,
    step=1,
    value=2023
    )

  select_all_rounds = st.toggle(
    label="Show All Rounds",
  )
  return selected_player, selected_round, selected_year, select_all_rounds

def app(name:str = ""):
  """Tennis Data Application Frontend"""
  # Config
  st.markdown(f"<h2 align='center'>{name}</h2>", unsafe_allow_html=True)
  player, round, year, round_override = controls()
  tables = query_database("SELECT name FROM (DESCRIBE) WHERE name LIKE 'RAW%';")['name'].to_list()  
  match_data_raw = get_all_matches()

  # Data
  if round_override:
    match_data = match_data_raw[match_data_raw["tourney_date"].dt.year == year]
    slam_data = pd.concat([query_database(f"""SELECT * FROM {i} WHERE year = '{year}' ;""") for i in tables], axis=0
      ).sort_values(["year", "slam", "round"], ascending=[False, True, True])

  else:
    match_data = match_data_raw[(match_data_raw["tourney_date"].dt.year == year) & (match_data_raw["round_no"] == round)]
    slam_data = pd.concat([query_database(f"""SELECT * FROM {i} WHERE year = '{year}' AND round = '{round}' ;""") for i in tables], axis=0
      ).sort_values(["year", "slam", "round"], ascending=[False, True, True])

  # Data Tabs  
  matches_tab, slam_tab, predict_tab = st.tabs(["🌎 Tour Matches", "🏆 Grand Slams", "🎓 AI Model"])
  
  if player == "All":
    slam_tab.data_editor(slam_data, use_container_width=True)
    matches_tab.data_editor(match_data, use_container_width=True)

    predict_tab.markdown("### COMING SOON...")

  else:
    player_key = f"{player.split(" ")[0][0]}. {player.split(" ", maxsplit=1)[1]}"
    match_data = match_data[(match_data["winner_name"] == player) | (match_data["loser_name"] == player)]
    slam_data = slam_data[(slam_data['player1'].str.contains(player_key)) | (slam_data['player2'].str.contains(player_key)) ]
    
    slam_tab.data_editor(slam_data, use_container_width=True)
    matches_tab.data_editor(match_data, use_container_width=True)

    predict_tab.markdown("### COMING SOON...")

  # Charts 
  if player != "All":
    st.markdown(f"<h4 align='center'>Rank History</h4>", unsafe_allow_html=True)
    points_tab, rank_tab = st.tabs(["Ranking Points", "Rank"])
    
    with points_tab:
      rank_data = pd.concat([
        match_data_raw[match_data_raw["winner_name"] == player][["tourney_date", "winner_rank_points"]].rename(columns={"winner_rank_points": "Points"}),
        match_data_raw[match_data_raw["loser_name"] == player][["tourney_date", "loser_rank_points"]].rename(columns={"loser_rank_points": "Points"}),
      ], axis=0).sort_values("tourney_date").rename(columns={"tourney_date": "Date"})

      st.scatter_chart(rank_data, x = "Date", y="Points", color=["#025719"])

    with rank_tab:
      rank_data = pd.concat([
        match_data_raw[match_data_raw["winner_name"] == player][["tourney_date", "winner_rank"]].rename(columns={"winner_rank": "Rank"}),
        match_data_raw[match_data_raw["loser_name"] == player][["tourney_date", "loser_rank"]].rename(columns={"loser_rank": "Rank"}),
      ], axis=0).sort_values("tourney_date").rename(columns={"tourney_date": "Date"})

      st.scatter_chart(rank_data, x = "Date", y="Rank", color=["#025719"])

if __name__ == "__main__":
  app()