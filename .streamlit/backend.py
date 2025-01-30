import datetime as dt, requests as req, polars as pl, pandas as pd

class Backend:
    def __init__(self,
        db_path: str = '../database/pipeline.duckdb', 
        events: list = ['ausopen', 'frenchopen', 'wimbledon', 'usopen'], 
        years: list = [i for i in range(2011, dt.datetime.today().year)],
    ):
        self.db = db_path
        self.events = events
        self.years = years
        self.rank  = self.rank_data(start_year=min(self.years))

    def rank_data(self, start_year: int = 1968, end_year: int = dt.datetime.now().year + 1):
        self.start_year = start_year
        self.end_year = end_year

        # Select base URL
        urls = [
            'https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_', 
            'https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_', ]

        url_zip = list(map(zip(urls, list(range(self.start_year, self.end_year)))))
        
        for idx, url in enumerate(urls):
            for n, i in enumerate(range(self.start_year, self.end_year)):
                n = (idx + 1) * n 
                url_end = '{}.csv'.format(i)
                response = req.get(url + url_end)

                if response.status_code == 200:
                    df_iter = pl.DataFrame(response.json())

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

    def match_data():
        pass