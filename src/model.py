# Models
import os, datetime as dt, polars as pl
from sklearn.model_selection import train_test_split
from sklearn import metrics

import keras
from keras import Sequential
from keras.src.layers import Dense
from keras.src.callbacks import EarlyStopping

from tensorflow.python.keras.engine import data_adapter

def _is_distributed_dataset(ds):
    return isinstance(ds, data_adapter.input_lib.DistributedDatasetSpec)

data_adapter._is_distributed_dataset = _is_distributed_dataset

# Build model
class NeuralModel:
    '''Methods for loading data, building a neural model, and running predictions against a saved model'''
    @staticmethod
    def create_feature_table(dir:str) -> pl.DataFrame:
        # Consolidate event tables
        files = [pl.read_parquet(f"{dir}/{i}") for i in os.listdir(dir) if i.endswith(".parquet")]
        table =  pl.concat(files, how="diagonal_relaxed")
        table = table.with_columns(
            serve_mph = pl.when(pl.col("Speed_MPH").is_not_null()).then(pl.col("Speed_MPH")).otherwise((pl.col("Speed_KMH").cast(pl.Int64) / 1.60934).cast(pl.Int64)),
            rally_count = pl.when(pl.col("Rally").is_not_null()).then(pl.col("Rally")).otherwise(pl.col("RallyCount")),
        ).drop(["Speed_MPH", "Speed_KMH", "Rally", "RallyCount"])

        # Standardise types
        feature_schema = {
            'match_id' : pl.String,
            'surface' : pl.String,
            'player1' : pl.String,
            'player2' : pl.String,
            'winner' : pl.Int32,
            'round' : pl.Int32,
            'age' : pl.Float64,
            'rank' : pl.Int64,
            'rank_points' : pl.Int64,
            'age_p2' : pl.Float64,
            'rank_p2' : pl.Int64,
            'rank_points_p2' : pl.Int64,
            'ElapsedTime' : pl.String,
            'SetNo' : pl.Int64,
            'P1GamesWon' : pl.Int32,
            'P2GamesWon' : pl.Int32,
            'SetWinner' : pl.Int32,
            'GameNo' : pl.Int32,
            'GameWinner' : pl.Int32,
            'PointNumber' : pl.Int32,
            'PointWinner' : pl.Int32,
            'PointServer' : pl.Int32,
            'ServeIndicator' : pl.Int32,
            'P1Score' : pl.Int32,
            'P2Score' : pl.Int32,
            'P1PointsWon' : pl.Int64,
            'P2PointsWon' : pl.Int64,
            'P1Ace' : pl.Int32,
            'P2Ace' : pl.Int32,
            'P1Winner' : pl.Int32,
            'P2Winner' : pl.Int32,
            'P1DoubleFault' : pl.Int32,
            'P2DoubleFault' : pl.Int32,
            'P1UnfErr' : pl.Int32,
            'P2UnfErr' : pl.Int32,
            'ServeNumber' : pl.Int32,
            'P1DistanceRun' : pl.Float64,
            'P2DistanceRun' : pl.Float64,
            'serve_mph' : pl.Int64,
            'rally_count' : pl.Int32,
        }
        table = table.cast(feature_schema)
        
        # Create features
        match_winners = {i["match_id"] : i["winner"] for i in pl.read_csv("data/static/match_winners.csv").select("match_id", "winner").to_dicts()}
        match_winners = {k: (2 if v == 0 else v) for k,v in match_winners.items()} # has 1 for p1 win & 0 for p2 win
        max_age = 50 # no player ever reaches
        max_points = 18_000 # 16,950 is record
        surface_map = {
            "clay": -1,
            "hard": 0,
            "grass": 1,
        }

        table = table.with_columns(
            year = pl.col("match_id").map_elements(lambda x: int(str(x).split("-")[0]), pl.Int64),
            surface = pl.col("surface").replace_strict(surface_map),
            ServeIndicator = pl.when(pl.col("ServeIndicator").is_null()).then(pl.col("ServeNumber")).otherwise(pl.col("ServeIndicator")),
            winner = pl.when(pl.col("winner").is_null()
                ).then(pl.col("match_id").str.replace("MS", "1").str.replace("WS", "2").replace_strict(match_winners, default=pl.col("winner"))
                ).otherwise(pl.col("winner")),
        )

        table = table.group_by("match_id").agg(
            winner = pl.col("winner").mean().cast(pl.Int32),
            year = pl.col("year").mean().cast(pl.Int64) - 2011,
            surface = pl.col("surface").mean().cast(pl.Int32),

            p1_age = pl.col("age").mean() / max_age,
            p2_age = pl.col("age_p2").mean() / max_age,

            p1_rank_points = pl.col("rank_points").mean() / max_points,
            p2_rank_points = pl.col("rank_points_p2").mean() / max_points,

            p1_first_won = (
                pl.col("PointServer").filter((pl.col("PointServer") == 1) & (pl.col("PointWinner") == 1) & (pl.col("ServeIndicator") == 1)).count() /
                pl.col("PointServer").filter((pl.col("PointServer") == 1) & (pl.col("ServeIndicator") == 1)).count()
            ),
            p1_secnd_won = (
                pl.col("PointServer").filter((pl.col("PointServer") == 1) & (pl.col("PointWinner") == 1) & (pl.col("ServeIndicator") == 2)).count() /
                pl.col("PointServer").filter((pl.col("PointServer") == 1) & (pl.col("ServeIndicator") == 2)).count()
            ),
            p2_first_won = (
                pl.col("PointServer").filter((pl.col("PointServer") == 2) & (pl.col("PointWinner") == 2) & (pl.col("ServeIndicator") == 1)).count() / 
                pl.col("PointServer").filter((pl.col("PointServer") == 2) & (pl.col("ServeIndicator") == 1)).count()
            ),
            p2_secnd_won = (
                pl.col("PointServer").filter((pl.col("PointServer") == 2) & (pl.col("PointWinner") == 2) & (pl.col("ServeIndicator") == 2)).count() / 
                pl.col("PointServer").filter((pl.col("PointServer") == 2) & (pl.col("ServeIndicator") == 2)).count()),
            
            p1_win_err = pl.col("P1Winner").sum() / pl.when(pl.col("P1UnfErr").sum() == 0).then(pl.lit(1)).otherwise(pl.col("P1UnfErr").sum()),
            p2_win_err = pl.col("P2Winner").sum() / pl.when(pl.col("P2UnfErr").sum() == 0).then(pl.lit(1)).otherwise(pl.col("P2UnfErr").sum()),
        )
    
        filter_cols = [i for i in table.columns if i not in ["match_id", "winner"]]
        table = table.drop_nulls(filter_cols).drop_nans(filter_cols)
        
        return table.sort("match_id", descending=False)
    
    @staticmethod
    def load_data(dir:str, start_year: int = 2011, end_year: int = 2025, tournament_list: list = ['ausopen', 'frenchopen', 'wimbledon', 'usopen']) -> pl.DataFrame:
        return NeuralModel.create_feature_table(dir).filter(
            (pl.col("year") >= (start_year - 2011)) & 
            (pl.col("year") <= (end_year - 2011)) &
            (pl.col("match_id").str.contains_any(tournament_list))
        )

    @staticmethod
    def build(data: pl.DataFrame, save_model: bool = False) -> dict:
        ''' Function will build a sequential neural network, given a dataset (add details)...
            
            It returns a dictionary containing the timestamp, accuracy score, training dataset with model predictions, and the model itself.
            If the user specifies save_model = True then the model will be saved in a saved_model folder as SlamModel_{timestamp}
        '''
        timestamp = dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d')
        target = data.drop_nulls().select("winner").to_series().map_elements(lambda x: 0 if x == 2 else x).to_numpy()
        features = data.drop_nulls().drop(["match_id", "winner"]).to_numpy()

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
        es = EarlyStopping(monitor = 'loss', mode = 'min', verbose = 0, patience = 32)

        model.compile(optimizer = 'adagrad', loss = 'binary_crossentropy', metrics = ['accuracy'])
        model.fit(x_train, y_train, batch_size = 32, epochs = 128, verbose = 1, callbacks = es)

        # Apply model to test data and measure accuracy
        y_pred = model.predict(x_test) > 0.5

        model_accuracy = round(100 * metrics.accuracy_score(y_test, y_pred),2)

        # Append predictions to base data and save
        data_pred = [i[0] for i in model.predict(data.drop(["match_id", "winner"]).to_numpy())]
        data = pl.concat([data, pl.DataFrame({"P1_win": data_pred})], how="horizontal")
        data = data.with_columns(P2_win = 1 - pl.col('P1_win'))
        
        # Save
        if save_model == True:
           model.save(f'models/dropshot.keras')
        
        return {
            'timestamp': timestamp,
            'dataset': data,
            'model summary': model.summary(),
            'model': model,
            'accuracy': model_accuracy
        }
    
    @staticmethod
    def pred(model, data:pl.DataFrame) -> pl.DataFrame:
        if isinstance(model, str):
            model = keras.models.load_model(model)
        
        data_pred = [i[0] for i in model.predict(data.drop(["match_id", "winner"]).to_numpy())]
        return pl.concat(
            items=[data, pl.DataFrame({"P1_win": data_pred})], 
            how="horizontal"
        ).with_columns(
            P2_win = 1 - pl.col('P1_win'),
        )


if __name__ == '__main__':   
    # Build a model
    model = NeuralModel.build(
        data = NeuralModel.load_data(
            dir="data/events", 
            start_year=2011, 
            end_year=2021, 
            tournament_list=['ausopen', 'frenchopen', 'wimbledon', 'usopen']
        ), 
        save_model = True
    )
    print(f'''Accuracy: {model['accuracy']}''')
    print(model["dataset"])

    # Run a model
    predictions = NeuralModel.pred(
        model="models/dropshot.keras", 
        data = NeuralModel.load_data(
            dir="data/events", 
            start_year=2022, 
            end_year=2025, 
            tournament_list=['ausopen', 'frenchopen', 'wimbledon', 'usopen']
        ),
    )
    with pl.Config(tbl_cols=-1):
        print(predictions)

    predictions.write_csv("data/dropshot.csv")
