# Models
import sys, datetime as dt, polars as pl, tensorflow as tf, tensorflow_hub as hub
from sklearn.model_selection import train_test_split
from sklearn import metrics

from tensorflow.python.keras import Sequential
from tensorflow.python.keras.layers import Dense
from tensorflow.python.keras.callbacks import EarlyStopping

# removing
import requests, pandas as pd

sys.path.append('../')
from src.pipelines.database import DatabaseManager


# Build model
class NeuralModel:
    '''Methods for loading data, building a neural model, and running predictions against a saved model'''
    @staticmethod
    def load(db_path: str, start_year: int = 2011, end_year: int = 2022, tournament_list: list = ['ausopen', 'frenchopen', 'wimbledon', 'usopen']):
        conn = DatabaseManager(db_path=db_path)
        event_list = []

        # Generate list of available datasets
        for year in range(start_year, end_year + 1):
            for tourney in tournament_list:
                if requests.get(f'https://raw.githubusercontent.com/JeffSackmann/tennis_slam_pointbypoint/master/{year}-{tourney}-matches.csv').status_code == 200:
                    event_list.append((year, tourney))
        
        for n, (year, tourney) in enumerate(event_list):        
            temp = conn.execute(f'''SELECT * FROM FEATURES_{year}_{tourney};''').fetch_df()
            
            if n == 0:
                data = temp
            else:
                data = pd.concat([data.copy(), temp.copy()], axis=0)

        return data.dropna().reset_index(drop=True)

    @staticmethod
    def build(data: pd.DataFrame, save_model: bool = False):
        ''' Function will build a sequential neural network, given a dataset (add details)...
            
            It returns a dictionary containing the timestamp, accuracy score, training dataset with model predictions, and the model itself.
            If the user specifies save_model = True then the model will be saved in a saved_model folder as SlamModel_{timestamp}
        '''
        timestamp = dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d')
        target = data.iloc[:, 6].to_numpy()
        features = data.iloc[:, 7:].to_numpy()

        # Split data
        x_train, x_test, y_train, y_test = train_test_split(features, target, test_size=0.1, random_state=0)

        # Model build
        model = Sequential()

        model.add(Dense(units = 256, activation = 'relu', input_shape = (features.shape[1],)))
        model.add(Dense(units = 128, activation = 'relu'))
        model.add(Dense(units = 64, activation = 'relu'))
        model.add(Dense(units = 32, activation = 'relu'))
        model.add(Dense(units = 16, activation = 'relu'))
        model.add(Dense(units = 1,  activation = 'sigmoid'))

        # Complie model and with early stopping function
        es = EarlyStopping(monitor = 'loss', mode = 'min', verbose = 0, patience = 128)

        model.compile(optimizer = 'adagrad', loss = 'binary_crossentropy', metrics = ['accuracy'])
        model.fit(x_train, y_train, batch_size = 127, epochs = 512, verbose = 0, callbacks = es)

        # Apply model to test data and measure accuracy
        y_pred = model.predict(x_test) > 0.5

        model_accuracy = round(100 * metrics.accuracy_score(y_test, y_pred),2)

        # Append predictions to base data and save
        data['P1 win'] = model.predict(data.iloc[:,7:].to_numpy())
        data['P2 win'] = 1 - data['P1 win'] 

        output = {
            'timestamp': timestamp,
            'dataset': data,
            'model summary': model.summary(),
            'model': model,
            'accuracy': model_accuracy
        }
        
        # Save
        if save_model == True:
            model.save(f'../models/model_assets_{timestamp}')
        
        return output

    @staticmethod
    def predict(input_df_loc: str = r'2022/US22_input_live_all_data.csv', save_output: bool = True):

        # Import & apply model 
        model = tf.keras.Sequential([
            hub.KerasLayer('slam_model')
        ])

        data = pd.DataFrame(pd.read_csv(input_df_loc))

        output = model(data.iloc[:,7:].copy().to_numpy())
        data['p1_win_pred'] = output
        data['p2_win_pred'] = 1 - output

        # Save results
        if save_output:
            timestamp = dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d')
            data.to_csv(fr'2022/US_SlamModel_live_{timestamp}.csv')
        else:
            return data

# Generte model input for tournament draw 
class FeatureMap:
    '''
    Calculates SlamModel features for a provided draw (raw_input) & match data (raw_source). 
    '''
    def __init__(self, raw_input: pd.DataFrame, raw_source: pd.DataFrame, filter_map: dict) -> None:
        self.input = raw_input # data to be mapped to
        self.source = raw_source # data to source from
        self.source_group = None # for create_grouping method
        self.filter_map = filter_map # dict( col name : list(filter values) )

    def filter_source(self):
        # filter source
        for key in self.filter_map.keys():
            self.source = self.source[ self.source[key].isin(self.filter_map[key]) ].copy()

        return self
    
    def create_grouping(self):
        inv_cols = [
            str(col).replace('1', '2') if str(col).find('1') != -1 else 
            str(col).replace('2', '1') if str(col).find('2') != -1 else 
            col for col in self.source.columns]

        source_inv = self.source.copy()
        source_inv.columns = inv_cols

        self.source = pd.concat([self.source, source_inv], axis = 0)
        return self

    def map_data(self):
        # add data to slam input
        source_group = self.source.groupby('player1')
        group_sum = source_group.sum().copy()
        group_mean = source_group.mean().copy()

        # calcs for player 2 in self.input are done on player 1 in source_group as it is indexed on player 1
        for n, player in enumerate(['player1', 'player2']):
            n += 1 # for col mapping below

            for row, i in enumerate(self.input[player]):
                try:
                    group_sum.loc[i, 'P1 Sets'] 
                except KeyError:
                    pass
                else:
                    self.input.loc[row, f'P{n} Sets'] = group_sum.loc[i, 'P1 Sets'] / group_sum.loc[i, 'Total Sets']
                    self.input.loc[row, f'P{n} Games'] = group_sum.loc[i, 'P1 Games'] / group_sum.loc[i, 'Total Games']
                    self.input.loc[row, f'P{n} Points'] = group_sum.loc[i, 'P1 Points'] / group_sum.loc[i, 'Total Points']
                    
                    # service / revieving points
                    self.input.loc[row, f'P{n} Points S'] = (
                        group_sum.loc[i, 'P1 first serves won'] + group_sum.loc[i, 'P1 second serves won']
                        ) / group_sum.loc[i, 'P1 Service Points']
                    
                    
                    self.input.loc[row, f'P{n} Points R'] = (
                        (group_sum.loc[i, 'P2 Service Points'] 
                        - group_sum.loc[i, 'P2 first serves won'] 
                        - group_sum.loc[i, 'P2 second serves won'])
                        / group_sum.loc[i, 'P2 Service Points']
                        )

                    # serve %'s 
                    self.input.loc[row, f'P{n} First S'] = group_sum.loc[i, 'P1 first serves won'] / group_sum.loc[i, 'P1 first serves']
                    self.input.loc[row, f'P{n} Second S'] = group_sum.loc[i, 'P1 second serves won'] / group_sum.loc[i, 'P1 second serves']

                    # winner to unforced error
                    self.input.loc[row, f'P{n} Win Unf'] = group_sum.loc[i, 'P1Winner'] / group_sum.loc[i, 'P1UnfErr']

                    # Mean serve speed 
                    self.input.loc[row, f'P{n} Mean Serve KMH'] = (
                        (group_mean.loc[i, 'P1 Mean Serve KMH'] - source_group.min().loc[i, 'P1 Mean Serve KMH'].min()) 
                        / (source_group.max().loc[i, 'P1 Mean Serve KMH'] - source_group.min().loc[i, 'P1 Mean Serve KMH']) )

        return self

    def remove_nulls(self):
        # fill NaN's from passing on missing players
        for col in self.input.columns[10:]:
            self.input[col] = [self.input[col].quantile(1/3) if pd.isna(i) else i for i in self.input[col]]

        return self

    def save(self, path: str):
        self.input.to_csv(path_or_buf=path, index=None)
        return self

    def run(self, path:str, drop_nulls: bool = True, save_output: bool = False):
    # Apply all methods 
        self.filter_source()
        self.create_grouping()
        self.map_data()

        if drop_nulls:
            self.remove_nulls()

        if save_output:
            self.save(path=path)

        return self.input


if __name__ == '__main__':
    from time import time 
    start = time()

    # Build model test
    data = NeuralModel.load(db_path='../database/FeatureStore.duckdb')
    model = NeuralModel.build(
        data=data[data['match_num']<2000].copy().reset_index(drop=True), 
        save_model = True)
    
    print(f'''Completed build time: {round((time() - start) / 60, 1)}s''')
    print(f'''Accuracy: {model['accuracy']}''')

    # # Generate draw data test
    # draw_data = pd.read_csv(r'../../data/US22_input.csv')
    # feature_data = pd.read_csv(r'../../data/baseline_clean.csv') # all match data from 2011
    # filter_map = {
    #     'slam': ['usopen'],
    #     'year': [2018, 2019, 2020, 2021, 2022],
    #     }

    # test = FeatureMap(raw_input=draw_data, raw_source=feature_data, filter_map=filter_map).run(path=None)
    