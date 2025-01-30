# Pipeline Manager
import datetime as dt, numpy as np, pandas as pd
from features import FeaturePipeline
from src.modules.database import DatabaseManager
from src.modules.rank import RankPipeline
from src.modules.raw import DataPipeline


class PipelineManager:
    '''Pipeline manager class... for managing data flows into DuckDB infra...'''
    def __init__(self, 
        db_path: str = '../database/pipeline.duckdb', 
        events: list = ['ausopen', 'frenchopen', 'wimbledon', 'usopen'], 
        years: list = [i for i in range(2011, dt.datetime.today().year)], ):
        
        self.PATH = db_path
        self.DATABASE = DatabaseManager(db_path = db_path) 
        self.events = events
        self.years = years

    def extract(self, save_raw: bool = True, save_baseline: bool = True):
        # Rank tables
        self.DATABASE.create_table(
            table_name='BASE_RANK_TABLE', 
            table_data=RankPipeline(start_year = min(self.years)).extract(), )

        # Match tables
        for year, event in [(i,j) for i in self.years for j in self.events]:
            DataPipeline(db_path=self.PATH, event = event, tour_year = year, save_raw=save_raw, save_baseline=save_baseline).run()
                
        return self
  
    def process(self):
        DATABASE_TABLES = self.DATABASE.execute('DESCRIBE;').fetch_df()['table_name'].to_list()
        outputs = []

        # Create feature tables
        for year, event in [(i,j) for i in self.years for j in self.events]:
            if np.isin(f'''BASE_{year}_{event}''', DATABASE_TABLES):
                source = self.DATABASE.execute(f'''SELECT * FROM BASE_{year}_{event}; ''').fetch_df()
                data = FeaturePipeline(data_source = source).run()

                outputs.append(data)
                self.DATABASE.create_table(table_name = f'''FEATURES_{year}_{event}''', table_data = data)
        
        # Create combined table 
        self.DATABASE.create_table(table_name = f'''FEATURES_ALL_EVENTS''', table_data = pd.concat(outputs, axis=0))
        return self

    def run(self):
        self.extract()
        self.process()
        return self

