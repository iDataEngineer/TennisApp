# Feature Pipeline

import pandas as pd

class FeaturePipeline:
    '''Feature transformation pipeline...'''
    def __init__(self, data_source) -> None:
        self.data = data_source
        self.df = pd.DataFrame()
        self.df_out = dict()
        self.output = pd.DataFrame()

    @staticmethod
    def ratio(value, total): 
        return value / total
    
    @staticmethod
    def min_max(value, min_val, max_val):
        return (value - min_val) / (max_val - min_val)

    def __get_data(self):
        # if file loc then read_csv, else use supplied dataframe
        if type(self.data) == str:
            self.df = pd.read_csv(self.data)
        else:
            self.df = self.data

        # remove un-usable cols
        self.df = self.df.drop(columns=['status', 'court_id', 'event_name', 'court_name', 'player1id','player2id','nation1', 'nation2'], axis = 1)

        # drop rows with missing player names - will cause key errors 
        self.df = self.df[(pd.isna(self.df['player1']) == False) & (pd.isna(self.df['player2']) == False)].copy().reset_index(drop=True)

    def __reference_map(self):
        df = self.df.copy().reset_index(drop = True)
        players = [i for i in pd.concat([df['player1'], df['player2'].rename('player1')], axis=0).dropna().unique()]

        # Create groupby object with all players in single col (duplicate of every df_raw line)
        cols_inv = [
            'match_id', 'year', 'slam', 'match_num', 'player2', 'player1', 'winner',
            'round', 'Total Sets', 'P2 Sets', 'P1 Sets', 'Total Games', 'P2 Games',
            'P1 Games', 'Total Points', 'P2 Points', 'P1 Points',
            'Total Service Points', 'P2 Service Points', 'P1 Service Points',
            'P2 first serves', 'P2 first serves won', 'P2 second serves',
            'P2 second serves won', 'P1 first serves', 'P1 first serves won',
            'P1 second serves', 'P1 second serves won', 'P2 Max Serve KMH',
            'P2 Mean Serve KMH', 'P1 Max Serve KMH', 'P1 Mean Serve KMH',
            'P2Winner', 'P1Winner', 'P2UnfErr', 'P1UnfErr', 'p2 rank points',
            'p1 rank points', 'p2 rank', 'p1 rank', 'p2 age', 'p1 age']

        df_inv = df.copy()
        df_inv.columns = cols_inv
        df_inv['winner'] = [0 if i == 1 else 1 for i in df_inv['winner']]

        df_grouped = pd.concat([df, df_inv], axis=0).groupby(by = ['year', 'slam', 'player1']).sum()

        # Create dict's with data for each year - slam across key metrics
        for year in self.df['year'].unique():
            for slam in self.df['slam'].unique():
                ys = f'{year}-{slam}'

                p1_sets = {}
                p1_games = {}
                p1_points, p1_points_s, p1_points_r = {},{},{}
                p1_1st, p1_2nd = {}, {}
                p1_win_unf = {}

                for player in players:
                    if (year, slam, player) not in df_grouped.index:
                        pass
                    else:
                        p1_sets.update({player: self.ratio(df_grouped.loc[(year, slam, player), 'P1 Sets'], 
                                        df_grouped.loc[(year, slam), 'Total Sets'][player]) })

                        p1_games.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P1 Games'][player], 
                                        df_grouped.loc[(year, slam), 'Total Games'][player]) })

                        p1_points.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P1 Points'][player], 
                                        df_grouped.loc[(year, slam), 'Total Points'][player]) })
                
                        p1_points_s.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P1 first serves won'][player] 
                                        + df_grouped.loc[(year, slam), 'P1 second serves won'][player], 
                                        df_grouped.loc[(year, slam), 'P1 Service Points'][player]) })
                
                        p1_points_r.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P2 Service Points'][player] 
                                        - df_grouped.loc[(year, slam), 'P2 first serves won'][player]
                                        - df_grouped.loc[(year, slam), 'P2 second serves won'][player], 
                                        df_grouped.loc[(year, slam), 'P2 Service Points'][player]) })

                        p1_1st.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P1 first serves won'][player], 
                                        df_grouped.loc[(year, slam), 'P1 first serves'][player]) })

                        p1_2nd.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P1 second serves won'][player], 
                                        df_grouped.loc[(year, slam), 'P1 second serves'][player]) })

                        p1_win_unf.update({ player: self.ratio(df_grouped.loc[(year, slam), 'P1Winner'][player], 
                                        df_grouped.loc[(year, slam), 'P1UnfErr'][player]) })


                self.df_out.update({ ys: [p1_sets, p1_games, p1_points, p1_points_s, p1_points_r, p1_1st, p1_2nd, p1_win_unf] })
    
    def __map_data(self):
        # Add year-slam ref to each row in feature df
        df_features = self.df.copy().reset_index(drop=True)
        
        ys_map = {}
        for i in df_features.index:
            ys = str(df_features.loc[i, 'year']) + '-' + str(df_features.loc[i, 'slam'])
            ys_map.update({i: ys})

        df_features['year slam'] = df_features.index.map(ys_map)

        # Map stats to feature df
        for i in df_features.index:
            for n in [1,2]:
                df_features.loc[i, f'P{n} Sets'] = self.df_out[ df_features.loc[i, 'year slam'] ][0][df_features.loc[i, f'player{n}']]
                df_features.loc[i, f'P{n} Games'] = self.df_out[df_features.loc[i, 'year slam']][1][df_features.loc[i, f'player{n}']]
                df_features.loc[i, f'P{n} Points'] = self.df_out[df_features.loc[i, 'year slam']][2][df_features.loc[i, f'player{n}']]
                df_features.loc[i, f'P{n} Points S'] = self.df_out[df_features.loc[i, 'year slam']][3][df_features.loc[i, f'player{n}']]
                df_features.loc[i, f'P{n} Points R'] = self.df_out[df_features.loc[i, 'year slam']][4][df_features.loc[i, f'player{n}']]
                df_features.loc[i, f'P{n} First S'] = self.df_out[df_features.loc[i, 'year slam']][5][df_features.loc[i, f'player{n}']] 
                df_features.loc[i, f'P{n} Second S'] = self.df_out[df_features.loc[i, 'year slam']][6][df_features.loc[i, f'player{n}']]
                df_features.loc[i, f'P{n} Win Unf'] = self.df_out[df_features.loc[i, 'year slam']][7][df_features.loc[i, f'player{n}']]

        # Remove unrequired cols and apply final scalings
        df_features['surface'] = df_features['slam'].map({'ausopen': 0, 'frenchopen': -1, 'wimbledon': 1, 'usopen': 0})
        df_features = df_features[['match_id', 'year', 'slam', 'match_num', 'player1', 'player2', 'winner', 'surface','round', 

                    'p1 age','p1 rank points','P1 Sets', 'P1 Games', 'P1 Points', 'P1 Points S', 
                    'P1 Points R','P1 First S', 'P1 Second S', 'P1 Win Unf', 'P1 Mean Serve KMH',

            
                    'p2 age', 'p2 rank points', 'P2 Sets', 'P2 Games', 'P2 Points', 'P2 Points S',
                    'P2 Points R', 'P2 First S', 'P2 Second S', 'P2 Win Unf', 'P2 Mean Serve KMH',
                    ]].copy()

        
        df_features['p1 rank points'] = df_features['p1 rank points'] / float(21500) # 21500 max - highest recorded 16950 ND-16
        df_features['p2 rank points'] = df_features['p2 rank points'] / float(21500)
        
        df_features['p1 age'] = self.min_max(df_features['p1 age'], float(15), float(45))
        df_features['p2 age'] = self.min_max(df_features['p2 age'], float(15), float(45))

        self.output = df_features

    def run(self):
        self.__get_data()
        self.__reference_map()
        self.__map_data()

        return self.output
