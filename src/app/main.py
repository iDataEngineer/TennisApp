# App 
import sys, re, pandas as pd, streamlit as st

sys.path.append('.')
from src.modules.database import DatabaseManager

class TennisApp:
    def __init__(self):
        # self.DATABASE = DatabaseManager('data/database/actions_match_data.duckdb')
        self.FEATURE_STORE = DatabaseManager('data/database/point-by-point.db')

    def auto_wide_mode(self):
        st.set_page_config(layout="wide")

    def header(self):
        st.markdown('<h1 align="center"><b> PureAero </b></h1>', unsafe_allow_html=True)
        # st.markdown('<h3 align="center"><b> ATP & WTA Match Stats & Predictions </b></h3>', unsafe_allow_html=True)
        st.markdown('---')

    def sidebar(self):
        st.sidebar.markdown('<h2 align="center"><b> App Controls </b></h2>', unsafe_allow_html=True)

        # Surface
        select_surface = st.sidebar.selectbox(label='Select Surface', options=['All', 'Hard', 'Clay', 'Grass'], index=0)
        setattr(self, 'select_surface', select_surface)

        # Players
        players = self.FEATURE_STORE.execute('SELECT player1, player2 FROM FEATURES_ALL_EVENTS; ').fetch_df()
        players = pd.concat([players['player1'], players['player2'].rename('player1')], axis=0).sort_values().unique().tolist()
        
        p1 = st.sidebar.selectbox(label='Select Player 1', options=players, index=players.index('R. Nadal'))
        p2 = st.sidebar.selectbox(label='Select Player 2', options=players, index=players.index('N. Djokovic'))
        
        setattr(self, 'player_1', p1)
        setattr(self, 'player_2', p2)

    def feature_data(self):
        # st.markdown('<h4 align="center"><b> Feature Data </b></h4>', unsafe_allow_html=True)
        
        surf_key = [0 if self.select_surface == 'Hard' else -1 if self.select_surface == 'Clay' else 1][0]
        data = self.FEATURE_STORE.execute('SELECT * FROM FEATURES_ALL_EVENTS; ').fetch_df()
        
        p1 = data[(data['player1'] == self.player_1) & (data['surface'] == surf_key)].sort_values('year', ascending=False)
        p1 = p1.copy().reset_index().loc[0, :][[i for i in p1.columns if re.search('player1|P1', i)]]

        p2 = data[(data['player1'] == self.player_2) & (data['surface'] == surf_key)].sort_values('year', ascending=False)
        p2 = p2.copy().reset_index().loc[0, :][[i for i in p2.columns if re.search('player1|P1', i)]]
        p2.index = [i.replace('1', '2') for i in p2.index]

        setattr(self, 'match_up', pd.concat([p1, p2], axis=0))
        return self

    def h2h_data(self):
        st.markdown('<h4 align="center"><b> Head to Head </b></h4>', unsafe_allow_html=True)
        
        # Get data
        atp_data = self.DATABASE.execute('SELECT * FROM ATP_matches; ').fetch_df()
        wta_data = self.DATABASE.execute('SELECT * FROM WTA_matches; ').fetch_df()
        data = pd.concat([atp_data, wta_data], axis=0).sort_values('tourney_date').drop_duplicates()

        # Filter for selection
        if self.select_surface != 'All':
            data = data[data['surface'] == self.select_surface].copy()

        player_regex = f'''{self.player_1.replace(' ', '*')}|{self.player_2.replace(' ', '*')}'''
        data = data[(data['winner_name'].str.contains(player_regex)) & (data['loser_name'].str.contains(player_regex))]

        data['tour_year'] = [int(i[:4]) for i in data['tourney_id']]
        data['tourney_date'] = pd.to_datetime(data['tourney_date'])
        data = data[['tour_year', 'tourney_date', 'tourney_name', 'surface', 'draw_size', 'round', 'minutes', 
                    'winner_name', 'loser_name', 'score', 'winner_rank', 'loser_rank']].copy().reset_index(drop=True)
        
        # Stats
        grp = data.groupby(['tour_year', 'winner_name', 'loser_name'])['score'].count().rename('match_wins').reset_index()

        ## win / loss
        st.markdown('<h5 align="center"><b> Matches </b></h5>', unsafe_allow_html=True)
        data_wl = (grp.groupby(['tour_year','winner_name']).sum().reset_index()
                        .pivot(index='tour_year', columns='winner_name', values='match_wins')
                        .fillna(0).T.astype(int))
        data_wl['total'] = data_wl.sum(axis=1)
        
        data_wl.columns = [str(i).replace('_', ' ').title() for i in data_wl.columns]

        st.dataframe(data_wl, use_container_width=True)
    
        ## longest match by year
        st.markdown('<h5 align="center"><b> Longest Matches </b></h5>', unsafe_allow_html=True)
        
        for n, year in enumerate(grp['tour_year'].unique()):
            tmp = pd.DataFrame(data[data['tour_year'] == year].sort_values('minutes', ascending=False).reset_index(drop=True).loc[0, :]).T.set_index('tour_year')

            if n == 0:
                data_time = tmp
            else:
                data_time = pd.concat([data_time, tmp], axis=0)
        
        data_time = data_time[['tourney_name', 'round', 'minutes', 'winner_name', 'score']].fillna(0).astype({'minutes': int}).T
        data_time.index = ['Tournament', 'Round', 'Duration (mins)', 'Winner', 'Score']
        data_time.columns = [str(i).replace('_', ' ').title() for i in data_time.columns]
        
        st.dataframe(data_time, use_container_width=True)

        ## all data
        all_data = data.copy()
        all_data.columns = [str(i).replace('_', ' ').title() for i in all_data.columns]
        
        st.markdown('<h5 align="center"><b> All Matches </b></h5>', unsafe_allow_html=True)
        
        with st.expander("Show Table"):
            st.dataframe(all_data, use_container_width = True )
        
        self.data = data.copy()
        return self

    def match_length_plot(self,):
        st.markdown('<h5 align="center"><b> Match Length Chart </b></h5>', unsafe_allow_html=True)
        match_dur = self.data.set_index('tourney_date')['minutes'].copy()

        with st.expander("Show Chart"):
            st.line_chart(match_dur, use_container_width=True)

    def run(self):
        self.auto_wide_mode()
        
        self.header()
        self.sidebar()

        self.feature_data()
        self.h2h_data()

        self.match_length_plot()
        

if __name__ == '__main__':
    TennisApp().run()