########################################################
################### Tennis Stats App ###################
########################################################

# Last update: 25-Jun-22

import datetime as dt, pandas as pd, streamlit as st, matplotlib.pyplot as plt
from utils.format import auto_wide_mode
from utils.preprocessor import app_data, player_list, filter_data, ranking_points
from utils.plotting import scatter_chart
from utils.preprocessor import data_processor, create_merged_data
from utils.images import get_header

# Run in wide mode
auto_wide_mode()

def pick_app():
    st.sidebar.header('App Selection')
    app = st.sidebar.selectbox(label='Selected App', options=['ATP Players', 'GS Forecasts'])
    st.sidebar.markdown('---')
    return app

def ATP_Tour():    
    ### Data ###
    data = app_data()
    players = player_list(data, ['winner_name', 'loser_name'])
    
    ### Sidebar ###
    st.sidebar.title('ATP Tour Tennis App')

    # App Timestamp - most recent tournament in DB 
    tour_dates = data.set_index('tourney_date', drop = True)['tourney_name'].to_dict()

    last_date_str = dt.datetime.strftime(max(tour_dates.keys()), '%b-%Y')
    last_tourney = tour_dates[max(tour_dates.keys())]
    last_surface = data[data['tourney_name'] == last_tourney]['surface'].unique()[0]

    st.sidebar.subheader(f'Most Recent Tournament:') 
    st.sidebar.info(f'{last_tourney} — {last_date_str} ({last_surface})')
    st.sidebar.markdown('---')

    # Sidebar filter header
    st.sidebar.subheader('App Filters:')

    # Player filter
    selected_player = st.sidebar.selectbox(label= 'Select Player', options= players, index=list(players).index('Carlos Alcaraz'))
    data = data[(data['winner_name'] == selected_player) | (data['loser_name'] == selected_player)].copy()

    # Year filter
    selected_year = st.sidebar.selectbox(label= 'Select year', options= filter_data(data, 'tourney_date', selected_player), index=0)
    if selected_year != 'All':
        data = data[data['tourney_date'].dt.year == selected_year]

    # Surface filter
    selected_surface = st.sidebar.selectbox(label= 'Select Surface', options= filter_data(data, 'surface', selected_player))
    if selected_surface != 'All':
        data = data[data['surface'] == selected_surface]

    # Round filter
    selected_round = st.sidebar.selectbox(label= 'Select Round', options= filter_data(data, 'round', selected_player))
    if selected_round != 'All':
        data = data[data['round'] == selected_round]

    # Tour level filter
    level_d = {'Grand Slam': 'G', 'Tour Finals': 'F', 'Masters 1000': 'M', 'ATP 250 & 500': 'A', 'Davis Cup': 'D',}
    levels = ['All']
    for lev in level_d.keys():
        levels.append(lev)
    selected_level = st.sidebar.selectbox(label= 'Select Tour Level', options=levels)

    if selected_level != 'All':
        data = data[data['tourney_level'] == level_d[selected_level]]

    ###############################################
    ################## App Build ##################
    ###############################################

    # # Header image
    # st.image(get_header(new_width=800))
    st.markdown('---')

    # Make df's for matches won & loset
    player_data_w = data[(data['winner_name'] == selected_player)].copy()
    player_data_l = data[(data['loser_name'] == selected_player)].copy()    

    # KPI tiles
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric(label='Match Wins', value=len(player_data_w))

    with col2: 
        st.metric(label='Match Losses', value=len(player_data_l))

    with col3:
        win_perc = round(100 * len(player_data_w) / (len(player_data_w) + len(player_data_l)), 1)
        st.metric(label='Win %', value=win_perc)

    with col4:
        max_rank = min(player_data_w['winner_rank'].min(), player_data_l['loser_rank'].min())
        st.metric(label= 'Best Rank', value= int(max_rank))

    with col5:
        if player_data_w.index.max() > player_data_l.index.max() or len(player_data_l) == 0:
            current_rank = player_data_w.loc[player_data_w.index.max(), 'winner_rank']

        if player_data_l.index.max() > player_data_w.index.max() or len(player_data_w) == 0:
            current_rank = player_data_l.loc[player_data_l.index.max(), 'loser_rank']

        if pd.isna(current_rank):
            current_rank = 'No rank'
        else:
            current_rank = str(int(current_rank))
        
        st.metric(label='Rank at Last Match', value=current_rank)


    ### Rank Points Timeline ###
    st.markdown('---')
    st.subheader('Ranking Points Timeline ⏱')

    rank_df = ranking_points(player_data_w, player_data_l)
    st.area_chart(rank_df)

    ### Tournament Summary Data ###
    st.markdown('---')
    st.subheader('Tournament Summary 📝')
    
    wins = player_data_w['tourney_name'].value_counts().sort_values(ascending = False).rename('Match Wins')
    losses = player_data_l['tourney_name'].value_counts().sort_values(ascending = False).rename('Match Losses')

    tour_summary = pd.concat([wins, losses], axis=1)
    tour_summary['Time (hr)'] = [0 if pd.isna(i) else int(i) for i in tour_summary.index.map(data.groupby('tourney_name').sum()['minutes'].to_dict()) / 60]
    tour_summary['Champion'] = [0 if pd.isna(i) else int(i) for i in tour_summary.index.map(data[(data['round'] == 'F') & (data['winner_name'] == selected_player)].groupby('tourney_name').count()['round'].to_dict())]
    
    tour_summary['Match Wins'] = [0 if pd.isna(i) else int(i) for i in tour_summary['Match Wins']]
    tour_summary['Match Losses'] = [0 if pd.isna(i) else int(i) for i in tour_summary['Match Losses']]
    tour_summary['Total'] = tour_summary['Match Wins'] + tour_summary['Match Losses']
    
    tour_summary['Win %'] = round(100 * tour_summary['Match Wins'] / (tour_summary['Match Wins'] + tour_summary['Match Losses']), 2)
    tour_summary['Win %'] = [0 if pd.isna(i) else int(i) for i in tour_summary['Win %']]
    
    st.dataframe(tour_summary.sort_values('Champion', ascending=False))

    ### Time on Court ###
    st.markdown('---')
    st.subheader('Match Length Timeline ⏱')
    st.pyplot(scatter_chart(data))

    ### Rivals
    st.markdown('---')
    st.subheader('Frequent Match Ups 🤝🏻')
    st.markdown('---')

    st.write('Multiple Wins Over')
    rival_w = pd.Series(player_data_w['loser_name'].value_counts(), name='Matches Won').sort_values(ascending=False).head(20)
    st.bar_chart(rival_w)

    st.write('Multiple Losses To')
    rival_l = pd.Series(player_data_l['winner_name'].value_counts(), name='Matches Lost').sort_values(ascending=False).head(20)
    st.bar_chart(rival_l.T)


    ### Matches won / lost ###
    st.markdown('---')
    st.subheader('Matches Results 📋')
    st.markdown('---')

    st.write('Matches Won')
    if len(player_data_w) == 0:
        st.write('No Match Wins')
    else:
        keep_cols = ['loser_name', 'score', 'round', 'minutes', 'tourney_date', 'tourney_name', 'surface', 'age_diff']
        matches_won = player_data_w.sort_values(by = 'tourney_date', ascending = False)[keep_cols]
        matches_won['tourney_date'] = [dt.datetime.strftime(i, '%b %Y') for i in matches_won['tourney_date'] ]
        st.dataframe(matches_won.set_index('loser_name', drop = True))

    st.write('Matches Lost')
    if len(player_data_l) == 0:
        st.write('No Match Losses')
    else:
        keep_cols = ['winner_name', 'score', 'round', 'minutes', 'tourney_date', 'tourney_name', 'surface', 'age_diff']
        matches_lost = player_data_l.sort_values(by = 'tourney_date', ascending = False)[keep_cols]
        matches_lost['tourney_date'] = [dt.datetime.strftime(i, '%d-%B-%Y') for i in matches_lost['tourney_date'] ]
        st.dataframe(matches_lost.set_index('winner_name', drop = True)) 
    
###########

    
def GrandSlam():

    # Dataset
    @st.cache(show_spinner=False)
    def update_data():
        return data_processor()

    data = update_data()

    st.sidebar.header('🎮 App Filters 🎮')

    # players
    players = pd.concat([data['Player 1'], data['Player 2'] ], axis = 0).sort_values().unique()
    selected_player = st.sidebar.multiselect(label='Select Players', options=players)

    # slams
    slams = data['Grand Slam'].unique()
    selected_slam = st.sidebar.multiselect(label='Select Slams', options=slams)

    # years
    years = data['Year'].unique()
    selected_year = st.sidebar.multiselect(label='Select Years', options=years)

    # round
    rounds = data['Round'].unique()
    selected_round = st.sidebar.multiselect(label='Select Round', options=rounds)

    #######################
    #### Apply Filters ####
    #######################

    # players
    if len(selected_player) != 0:
        data = data[(data['Player 1'].isin(selected_player)) | (data['Player 2'].isin(selected_player))]

    # slams
    if len(selected_slam) > 0:
        data = data[data['Grand Slam'].isin(selected_slam)]

    # years
    if len(selected_year) > 0:
        data = data[data['Year'].isin(selected_year)]
    else:
        data = data.drop(columns=['Year'])

    # rounds
    if len(selected_round) > 0:
        data = data[data['Round'].isin(selected_round)]


    ############################################
    ################ APP Build #################
    ############################################
    st.header('Grand Slam Predictions')

    st.markdown ('''
        This app is used for sharing the output from my SlamModel project I run throughout the year.
        ''')

    # Header Image & Source
    st.image(get_header())
    st.markdown('Image: [Josh Calabrese](https://images.unsplash.com/photo-1482614312710-79c1d29bda2a?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&auto=format&fit=crop&w=1170&q=80)')


    # Model Accuracy Chart
    st.subheader('Model Performance...')
    accuracy_round = data.groupby(by=['Round']).mean()['Predicted']*100
    accuracy_overall = round(accuracy_round.sum() / 7, 1)

    st.markdown(f'### Mean model accuracy: {accuracy_overall} %')
    st.bar_chart(accuracy_round)

    # Player Results
    st.subheader('Player Results...')
    data_merge = create_merged_data(dataset=data)
    df_players = pd.DataFrame(index = data_merge.mean().index)

    df_players['Matches Played'] = df_players.index.map(data_merge.count()['Predicted'].to_dict())
    df_players['Predicted'] = df_players.index.map(data_merge.sum()['Predicted'].to_dict()).astype(int)

    df_players['% Accuracy'] = 100 * df_players['Predicted'] / df_players['Matches Played']
    df_players['% Accuracy'] = [int(i) for i in df_players['% Accuracy']]

    df_players['Mean Model Win %'] = df_players.index.map(data_merge.mean()['P1 Win %'].to_dict()).astype(int)
    df_players['Mean Model Lose %'] = df_players.index.map(data_merge.mean()['P2 Win %'].to_dict()).astype(int)

    df_players['Last Round Played'] = df_players.index.map(data_merge.max()['Round'].to_dict())

    # filter 
    if len(selected_player) > 0:
        df_players = df_players[df_players.index.isin(selected_player)]

    st.dataframe(df_players.sort_values(by = ['Predicted', '% Accuracy'], ascending=False))

    # Filtered Data
    st.subheader('Filtered Results...')
    st.dataframe(data)

if __name__ == '__main__':
    app = pick_app()
    if app == 'ATP Players':
        ATP_Tour()
    elif app == 'GS Forecasts':
        GrandSlam()
