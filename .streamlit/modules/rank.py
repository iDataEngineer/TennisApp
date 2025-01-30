# Rank Pipeline

import datetime as dt, requests as req, pandas as pd

class RankPipeline:
    '''Rank data extract pipeline'''
    def __init__(self, start_year: int = 1968, end_year: int = dt.datetime.now().year + 1):
        self.start_year = start_year
        self.end_year = end_year

    def configre(self, urls: list | None = None):
        # Select base URL
        if urls == None:
            urls = [
                'https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_', 
                'https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_', ]

        # Create a list of file locations
        self.tour_files = []

        for url in urls:
            for i in range(self.start_year, self.end_year):
                url_end = '{}.csv'.format(i)
                self.tour_files.append(url + url_end)
        
        return self

    def extract(self, urls: list | None = None):
        # Select base URL
        self.configre(urls=urls)

        for n, file in enumerate(self.tour_files):
            if req.get(file).status_code == 200:
                df_iter = pd.read_csv(file, parse_dates=['tourney_date'])
                
                if n == 0:
                    df_tour = df_iter
                else:
                    df_tour = pd.concat([df_tour, df_iter])
        
        return_cols = [
            'tourney_name', 'tourney_date', 'surface', 
            'winner_name', 'winner_age', 'winner_rank', 'winner_rank_points', 
            'loser_name', 'loser_age', 'loser_rank', 'loser_rank_points', ]

        return df_tour[return_cols].copy().reset_index(drop = True)

