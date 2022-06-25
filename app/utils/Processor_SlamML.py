# SlamApp Data Processing Helper Functions 
import pandas as pd
def data_processor():
    urls = [
            'https://raw.githubusercontent.com/iDataEngineer/ATP-SlamApp/main/data/SlamModel_output.csv',
    ]
    
    # extract
    data = pd.read_csv(urls[0], index_col=0).reset_index(drop = True)
    
    if len(urls) > 1:
        for url in urls[1:]:
            temp = pd.read_csv(url, index_col=0)
            data = pd.concat([data, temp], axis=0).reset_index(drop=True)

    # transform
    data.columns = ['Year', 'Grand Slam', 'Match No', 'Player 1', 
                    'Player 2', 'Winner', 'Surface', 'Round', 'P1 Win %', 
                    'P2 Win %', 'SlamModel Forecast']
    
    # simplify match numbers
    data['Match No'] = data.index + 1

    # Convert to %
    data['P1 Win %'] = [int(round(100 * i, 1)) for i in  data['P1 Win %']]
    data['P2 Win %'] = [int(round(100 * i, 1)) for i in  data['P2 Win %']]
    
    # Slam names
    slam_map = {'ausopen': 'Australian Open', 'frenchopen': 'Roland Garros', 'wimbledon': 'The Championships', 'usopen': 'US Open'}
    data['Grand Slam'] = data['Grand Slam'].map(slam_map)

    # Pred check
    data['Predicted'] = [1 if data.loc[i, 'Winner'] == data.loc[i, 'SlamModel Forecast'] else 0 for i in data.index]
    return data


def create_merged_data(dataset):
    data_merge = dataset.copy()

    # make cols consistent for grouping
    if 'Year' in dataset.columns:
        data_merge.columns = [  'Year', 'Grand Slam', 'Match No', 
                                'Player 2', 'Player 1', 'Winner', 'Surface', 'Round', 
                                'P2 Win %', 'P1 Win %', 'SlamModel Forecast', 'Predicted'
                                ]
    else:
        data_merge.columns = [  'Grand Slam', 'Match No', 'Player 2', 'Player 1', 
                                'Winner', 'Surface', 'Round', 'P2 Win %', 
                                'P1 Win %', 'SlamModel Forecast', 'Predicted']

    data_merge = pd.concat([dataset, data_merge], axis=0).groupby(by = 'Player 1')
    return data_merge