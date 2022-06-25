# Data Processor
import pandas as pd, streamlit as st, datetime as dt

### Functions ###

# Wide Mode 
st.cache(show_spinner=False)
def auto_wide_mode():
    st.set_page_config(layout="wide")


# Update dataset
st.cache(show_spinner=False)
def app_data():
    data = pd.read_csv(r'https://raw.githubusercontent.com/iDataEngineer/DataEng-atp-stats/main/data/atp_matches_db.csv', parse_dates=['tourney_date'], index_col=0)
    data = data.drop(columns=['tourney_id', 'match_num', 'winner_id', 'loser_id'])

    data['winner_hand'] = data['winner_hand'].map({'R':'R', 'L':'L', 'U':'U', 0:'U'})
    data['loser_hand'] = data['loser_hand'].map({'R':'R', 'L':'L', 'U':'U', 0:'U'})
    data['tour_year'] = [i.year for i in data['tourney_date']]
    data['tourney_name'] = ['US Open' if i == 'Us Open' else i for i in data['tourney_name']]
    
    data['age_diff'] = round(data['winner_age'] - data['loser_age'], 0)
    data['age_diff'] = [0 if pd.isna(i) else int(i) for i in data['age_diff'] ]

    return data


# Convert dataframe to csv for user download
@st.cache
def convert_df(df):
    return df.to_csv().encode('utf-8')


# List of unique players
@st.cache
def player_list(df, col_list):    
    players = df[col_list[0]].copy()

    if len(col_list) > 1:
        for col in col_list[1:]:
            players = pd.concat([players, df[col] ], axis= 0)
    
    players.columns = ['players']
    return players.reset_index(drop = True).sort_values().unique()


# Apply filters to dataset 
@st.cache(show_spinner=False)
def filter_data(df, column_name, player):
    output = ['All']
    
    # extract unique years if date col, else just unique vals
    if column_name == 'tourney_date':
        dataset = df[(df['winner_name'] == player) | (df['loser_name'] == player)][column_name].dt.year.unique()
    else:
        dataset = df[(df['winner_name'] == player) | (df['loser_name'] == player)][column_name].unique()
    
    # update list 
    for i in dataset:
        output.append(i)
    
    return output


# Ranking points timeline
def ranking_points(data_w, data_l):
    rank_w = {}
    for i in data_w.index:
        date_string = dt.datetime.strftime(data_w.loc[i, 'tourney_date'], '%Y-%b-%d')
        rank_w[date_string] = data_w.loc[i, 'winner_rank_points']

    rank_l = {}
    for i in data_l.index:
        date_string = dt.datetime.strftime(data_l.loc[i, 'tourney_date'], '%Y-%b-%d')
        rank_l[date_string] = data_l.loc[i, 'loser_rank_points']

    rank_combined = rank_w | rank_l
    date_index = [pd.to_datetime(i) for i in rank_combined.keys()]
    return pd.DataFrame(index = date_index, data = rank_combined.values(), columns=['Ranking Points'])