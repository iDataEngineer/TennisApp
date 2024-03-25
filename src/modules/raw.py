# Raw Data Pipeline

import datetime as dt, requests as req, pandas as pd
from src.modules.rank import RankPipeline
from src.utils.pipelines import Utilities
from src.modules.database import DatabaseManager

class ExtractRaw:
    '''Raw data extract pipeline'''
    def __init__(self, event: str, dataset: str, tour_year: int, base_url: str = r'https://raw.githubusercontent.com/JeffSackmann/tennis_slam_pointbypoint/master/'):
        self.url_base = base_url 
        self.url = None
        self.table = None
        
        if event not in ['ausopen', 'frenchopen', 'wimbledon', 'usopen']:
            raise ValueError('Select "ausopen", "frenchopen", "wimbledon", "usopen" as event')
        else:
            self.event = event
        
        if dataset not in ['points', 'matches']:
            raise ValueError('Select "matches" or "points" data set')
        else:
            self.dataset = dataset
        
        if tour_year < 2011:
            raise ValueError('Tour year must be 2011 or later')
        else:
            self.year = tour_year
        
    def generate_url(self):
        url = f'{self.url_base}{self.year}-{self.event}-{self.dataset}.csv'
        if req.get(url).status_code != 200:
            pass
        else: 
            self.url = url

    def data_extract(self):
        '''Extracts data from URL and applies extract formatter'''
        if self.url is not None:
            self.table = pd.read_csv(self.url, low_memory=False).reset_index(drop=True)
        
        else:
            self.table = pd.DataFrame(columns=['no_data'])
    
    def run(self):
        self.generate_url()        
        self.data_extract()
        return self.table



class DataPipeline:
    '''Raw data transformation pipeline'''
    def __init__(self, db_path:str, event: str, tour_year: int, save_raw: bool = False, save_baseline: bool = False):
        self.run_time = dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d')
        
        self.db_path = db_path
        self.event = event
        self.year = tour_year 
        self.save_raw = save_raw
        self.save_baseline = save_baseline
        self.df_ref = pd.DataFrame()

        self.points = ExtractRaw(event=event, dataset='points', tour_year=tour_year).run()
        if list(self.points.columns)[0] == 'no_data':
            pass

        else:
            self.matches_df = Utilities.name_formatter(
                ExtractRaw(event=event, dataset='matches', tour_year=tour_year).run() )
            self.points_df = Utilities.points_totaliser(self.points)

    def set_stats(self, column, stat):
        ids = [i for i in self.matches_df['match_id'].unique()]
        sets_no = dict()
        sets_p1 = dict()
        sets_p2 = dict()

        for i in ids:
            total = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df[column].isin([1, 2]))])
            p1 = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df[column] == 1)])
            p2 = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df[column] == 2)])

            sets_no.update({i: total})
            sets_p1.update({i: p1})
            sets_p2.update({i: p2})
            
        self.matches_df['Total ' + stat] = self.matches_df['match_id'].map(sets_no)
        self.matches_df['P1 ' + stat] = self.matches_df['match_id'].map(sets_p1)
        self.matches_df['P2 ' + stat] = self.matches_df['match_id'].map(sets_p2)
        
        return self

    def player_stats(self, column):
        ids = [i for i in self.matches_df['match_id'].unique()]
        sets_no = dict()

        for i in ids:
            total = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df[column] == 1)])        
            sets_no.update({i: total})

        self.matches_df[column] = self.matches_df['match_id'].map(sets_no)

        return self

    def serve_counts(self, server):
        ids = [i for i in self.matches_df['match_id'].unique()]
        first_count = dict()
        first_count_won = dict()
        second_count = dict()
        second_count_won = dict()
        
        for i in ids:
            first = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df['PointServer'] == server) & (self.points_df['ServeIndicator'] == 1)])
            first_won = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df['PointServer'] == server) 
                            & (self.points_df['ServeIndicator'] == 1) & (self.points_df['PointWinner'] == server)])
            
            second = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df['PointServer'] == server) & (self.points_df['ServeIndicator'] == 2)]) 
            second_won = len(self.points_df[(self.points_df['match_id'] == i) & (self.points_df['PointServer'] == server) 
                            & (self.points_df['ServeIndicator'] == 2) & (self.points_df['PointWinner'] == server)])

            first_count.update({i: first})
            first_count_won.update({i: first_won})
            second_count.update({i: second})
            second_count_won.update({i: second_won})

        self.matches_df[f'P{server} first serves'] = self.matches_df['match_id'].map(first_count)
        self.matches_df[f'P{server} first serves won'] = self.matches_df['match_id'].map(first_count_won)
        self.matches_df[f'P{server} second serves'] = self.matches_df['match_id'].map(second_count)
        self.matches_df[f'P{server} second serves won'] = self.matches_df['match_id'].map(second_count_won)

        return self

    def serve_speeds(self, server):
        ids = [i for i in self.matches_df['match_id'].unique()]
        max_spd = dict()
        avg_spd = dict()

        for i in ids:
            m_spd = self.points_df[(self.points_df['match_id'] == i) & (self.points_df['PointServer'] == server)]['Speed_KMH'].max()   
            a_spd = self.points_df[(self.points_df['match_id'] == i) & (self.points_df['PointServer'] == server)]['Speed_KMH'].mean()

            max_spd.update({i: m_spd})
            avg_spd.update({i: a_spd})

        self.matches_df[f'P{server} Max Serve KMH'] = self.matches_df['match_id'].map(max_spd)
        self.matches_df[f'P{server} Mean Serve KMH'] = self.matches_df['match_id'].map(avg_spd)
    
        return self

    def correct_id_errors(self):
        self.matches_df['round'] = [int(str(i)[-3]) for i in self.matches_df['match_num']]

        # fix match numbers
        for n, i in enumerate(self.matches_df['match_num']):
            try:
                i[0]
            except TypeError:
                pass
            else:
                if i[0] == 'M':
                    self.matches_df.loc[n, 'match_num'] = int(i.split('S')[1]) + 1000
                if i[0] == 'W':
                    self.matches_df.loc[n, 'match_num'] = int(i.split('S')[1]) + 2000

            # fix match ids            
            self.matches_df.loc[n, 'match_id'] = f"{self.matches_df.loc[n, 'year']}-{self.matches_df.loc[n, 'slam']}-{self.matches_df.loc[n, 'match_num']}"
        
        return self

    def match_winners(self):
        for i in self.matches_df.index:
            if self.matches_df.loc[i, 'P1 Sets'] > self.matches_df.loc[i, 'P2 Sets']:
                    self.matches_df.loc[i, 'winner'] = 1
            
            if self.matches_df.loc[i, 'P2 Sets'] > self.matches_df.loc[i, 'P1 Sets']:
                    self.matches_df.loc[i, 'winner'] = 0
            
            if self.matches_df.loc[i, 'P1 Sets'] == self.matches_df.loc[i, 'P2 Sets']:
                if self.matches_df.loc[i, 'P1 Games'] > self.matches_df.loc[i, 'P2 Games']:
                    self.matches_df.loc[i, 'winner'] = 1
                
                if self.matches_df.loc[i, 'P2 Games'] > self.matches_df.loc[i, 'P1 Games']:
                    self.matches_df.loc[i, 'winner'] = 0
        
        return self
    
    def raw_output(self):
        if self.save_raw == True and self.matches_df.shape != (0,0):
            DatabaseManager(db_path=self.db_path).create_table(table_name=f'''RAW_{self.year}_{self.event}''', table_data=self.matches_df)
        
        return self

    def get_reference_data(self, url: str = 'https://raw.githubusercontent.com/DNYFZR/TennisApp/main/data/ATP_tour.csv'):
        '''reference dataset for tour year'''
        slam_map = {
            'Australian Open': 'ausopen', 
            'Roland Garros': 'frenchopen', 
            'Wimbledon': 'wimbledon', 
            'US Open': 'usopen',
            'Us Open': 'usopen'} 

        self.df_ref = RankPipeline(start_year=self.year, end_year=self.year + 1).extract()
        self.df_ref['tourney_name'] = self.df_ref['tourney_name'].map(slam_map)
        
        # filter on event
        self.df_ref = self.df_ref[
            (self.df_ref['tourney_name'] == self.event) & 
            (self.df_ref['tourney_date'].dt.year == self.year)
            ].copy().reset_index(drop=True)

        return self

    def clean_reference_data(self):
        self.df_ref = Utilities.name_formatter(self.df_ref, column_list=['winner_name', 'loser_name'])
        
        return self

    def map_reference_data(self): 
        # extract reference data - rank, points & age
        points, ranks, ages = {}, {}, {}

        for i, winner in enumerate(self.df_ref['winner_name']):
            if winner in points.keys():
                pass
            else:
                points.update({winner: self.df_ref.loc[i, 'winner_rank_points']}) 
                ranks.update({winner: self.df_ref.loc[i, 'winner_rank']})
                ages.update({winner: self.df_ref.loc[i, 'winner_age']})

        for i, loser in enumerate(self.df_ref['loser_name']):
            if loser in points.keys():
                pass
            else:
                points.update({loser: self.df_ref.loc[i, 'loser_rank_points']}) 
                ranks.update({loser: self.df_ref.loc[i, 'loser_rank']})
                ages.update({loser: self.df_ref.loc[i, 'loser_age']})

        # Map to base data
        p1_map_points, p1_map_rank, p1_map_age = {}, {}, {}
        p2_map_points, p2_map_rank, p2_map_age = {}, {}, {}

        for i in self.matches_df.index:
            player_1 = self.matches_df.loc[i, 'player1']
            player_2 = self.matches_df.loc[i, 'player2']

            if player_1 in points.keys():
                p1 = points[player_1]
                p1_n = ranks[player_1]
                p1_a = ages[player_1]

                p1_map_points.update({i: p1})
                p1_map_rank.update({i: p1_n})
                p1_map_age.update({i: p1_a})

            if player_2 in points.keys():
                p2 = points[player_2]
                p2_n = ranks[player_2]
                p2_a = ages[player_2]

                p2_map_points.update({i: p2})
                p2_map_rank.update({i: p2_n})
                p2_map_age.update({i: p2_a})

        self.matches_df['p1 rank points'] = self.matches_df.index.map(p1_map_points)
        self.matches_df['p2 rank points'] = self.matches_df.index.map(p2_map_points)

        self.matches_df['p1 rank'] = self.matches_df.index.map(p1_map_rank)
        self.matches_df['p2 rank'] = self.matches_df.index.map(p2_map_rank)

        self.matches_df['p1 age'] = self.matches_df.index.map(p1_map_age)
        self.matches_df['p2 age'] = self.matches_df.index.map(p2_map_age)
        
        return self

    def save_output(self):
        if self.save_baseline == True:
            DatabaseManager(db_path=self.db_path).create_table(table_name=f'''BASE_{self.year}_{self.event}''', table_data=self.matches_df)
        return self

    def run(self):
        names = [('SetWinner', 'Sets'), ('GameWinner', 'Games'),('PointWinner', 'Points'), ('PointServer', 'Service Points')]
        if list(self.points.columns)[0] != 'no_data':
            # Match stats 
            for col, stat in names:
                self.set_stats(column=col, stat=stat) 
            
            # Serve stats
            for i in [1, 2]: 
                self.serve_counts(server = i) 
                self.serve_speeds(server = i)
            
            # Winners & Unforced Error stats
                self.player_stats(f'P{i}Winner')
                self.player_stats(f'P{i}UnfErr')
        
            # transform_02
            self.correct_id_errors()
            self.match_winners()
            self.raw_output()

            # Add ref data to main set
            self.get_reference_data()            
            self.clean_reference_data()
            self.map_reference_data()

            self.save_output()
            return self.matches_df

