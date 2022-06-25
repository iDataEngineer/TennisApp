########################################################
################### Tennis Stats App ###################
########################################################

# Last update: 25-Jun-22

import datetime as dt, pandas as pd, streamlit as st, matplotlib.pyplot as plt
from utils.Processor_tour import auto_wide_mode, app_data, convert_df, player_list, filter_data, ranking_points
from utils.ChartFunctions import pie_plot

st.set_page_config(page_title="Plotting Demo", page_icon="📈")

# Run in wide mode
auto_wide_mode()

### Data ###
data = app_data()
raw_data = data.copy()
players = player_list(data, ['winner_name', 'loser_name'])
today = dt.datetime.strftime(dt.datetime.today(), '%Y-%B-%d')

### Sidebar ###

# App Timestamp - most recent tournament in DB 
tour_dates = data.set_index('tourney_date', drop = True)['tourney_name'].to_dict()

last_date = max(tour_dates.keys())
last_date_str = dt.datetime.strftime(last_date, '%b-%Y')
last_tourney = tour_dates[last_date]
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

# Make df's for matches won & loset
player_data_w = data[(data['winner_name'] == selected_player)].copy()
player_data_l = data[(data['loser_name'] == selected_player)].copy()    

###############################################
################## App Build ##################
###############################################

st.header('TennisApp ▶ ATP Player Stats')
st.markdown('---')

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
    
    st.metric(label='Rank at Last Match', value=str(int(current_rank)))


### Rank Points Timeline ###
st.markdown('---')
st.subheader('Ranking Points Timeline ⏱')
st.markdown('---')

rank_df = ranking_points(player_data_w, player_data_l)
st.area_chart(rank_df)


### Tournament Summary Data ###
st.markdown('---')
st.subheader('Tournament Summary 📝')
st.markdown('---')

wins = player_data_w['tourney_name'].value_counts().sort_values(ascending = False).rename('Match Wins')
losses = player_data_l['tourney_name'].value_counts().sort_values(ascending = False).rename('Match Losses')

wins_losses = pd.concat([wins, losses], axis=1)
wins_losses['Match Wins'] = [0 if pd.isna(i) else int(i) for i in wins_losses['Match Wins']]
wins_losses['Match Losses'] = [0 if pd.isna(i) else int(i) for i in wins_losses['Match Losses']]
wins_losses['Total'] = wins_losses['Match Wins'] + wins_losses['Match Losses']
wins_losses['Win %'] = round(100 * wins_losses['Match Wins'] / (wins_losses['Match Wins'] + wins_losses['Match Losses']), 2)
wins_losses['Win %'] = [0 if pd.isna(i) else int(i) for i in wins_losses['Win %']]
wins_losses['Time (hr)'] = [0 if pd.isna(i) else round(i,1) for i in wins_losses.index.map(data.groupby('tourney_name').sum()['minutes'].to_dict()) / 60]

st.dataframe(wins_losses)

### Time on Court ###
st.markdown('---')
st.subheader('Match Length Timeline ⏱')
st.markdown('---')

groups = data.groupby('tourney_level')

fig = plt.figure(figsize = (12,8))
level_d_inv = {v: k for k, v in level_d.items()}

for name, group in groups:
    name = level_d_inv[name]
    plt.plot(
        group['tourney_date'], 
        (group['minutes'] / 60).round(1), 
        marker='o', 
        linestyle='', 
        markersize=6, 
        label=name
        )

plt.legend()
plt.title('Match Duration', size=14)
plt.ylabel('Hours')
st.pyplot(fig)

### Matches won / lost ###
st.markdown('---')
st.subheader('Matches Results 📋')
st.markdown('---')

st.write('Matches Won')
if len(player_data_w) == 0:
    st.write('No Match Wins')
else:
    keep_cols = ['loser_name', 'age_diff','tourney_date', 'tourney_name', 'surface', 'round', 'score', 'minutes', 'sets_played']
    matches_won = player_data_w.sort_values(by = ['tourney_date', 'minutes'], ascending = False)[keep_cols]
    matches_won['tourney_date'] = [dt.datetime.strftime(i, '%d-%B-%Y') for i in matches_won['tourney_date'] ]
    st.dataframe(matches_won.set_index('loser_name', drop = True))

st.write('Matches Lost')
if len(player_data_l) == 0:
    st.write('No Match Losses')
else:
    keep_cols = ['winner_name', 'age_diff','tourney_date', 'tourney_name', 'surface', 'round', 'score', 'minutes', 'sets_played']
    matches_lost = player_data_l.sort_values(by = ['tourney_date', 'minutes'], ascending = False)[keep_cols]
    matches_lost['tourney_date'] = [dt.datetime.strftime(i, '%d-%B-%Y') for i in matches_lost['tourney_date'] ]
    st.dataframe(matches_lost.set_index('winner_name', drop = True))


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


# Set Stat Charts
player_data_w['score_split'] = [i.strip().split(' ') for i in player_data_w['score'].copy()]
player_data_l['score_split'] = [i.strip().split(' ') for i in player_data_l['score'].copy()]

scores_w, scores_l = [], []
for score_list in player_data_w['score_split']:
    for score in score_list:
        try: score.split('(') # check for tiebreak 7-6(3) type score
        except ValueError: scores_w.append(score) # if not of type, append to list as is
        else: scores_w.append(score.split('(')[0]) # if of type, extract then append

for score_list in player_data_l['score_split']:
    for score in score_list:
        try: score.split('(') # check for tiebreak 7-6(3) type score
        except ValueError: scores_l.append(score) # if not of type, append to list as is
        else: scores_l.append(score.split('(')[0]) # if of type, extract then append

scores_w = pd.Series(scores_w, name='% in Matches Won').value_counts(normalize=True).sort_values(ascending=False)
scores_w = scores_w[scores_w >= 0.05]
scores_l = pd.Series(scores_l, name='% in Matches Lost').value_counts(normalize=True).sort_values(ascending=False)
scores_l = scores_l[scores_l >= 0.05]

col8, col9 = st.columns(2)

# Matches Won Chart 
with col8:
    st.pyplot(pie_plot(selected_player, scores_w, outcome='won'))

# Matches Lost Chart 
with col9:
    st.pyplot(pie_plot(selected_player, scores_l, outcome='lost'))


# Download filter & button
st.sidebar.markdown('---')
st.sidebar.subheader('Download Data')
selected_download = st.sidebar.selectbox(label='Select Dataset', options=['Filtered data', 'Tournament data', 'Raw data'])

if selected_download == 'Filtered data':
    data_dl = data.sort_values(by='tourney_date')

if selected_download == 'Tournament data':
    data_dl = wins_losses

if selected_download == 'Raw data':
    data_dl = raw_data

st.sidebar.download_button(
    label="Download Selected Dataset", 
    data=convert_df(data_dl), 
    file_name='data.csv', 
    mime='text/csv')
