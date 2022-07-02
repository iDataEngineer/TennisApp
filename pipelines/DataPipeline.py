### ATP Tour Match Data Pipeline ###
import pandas as pd, datetime as dt

def run_pipeline(start_year = 1968, end_year = dt.datetime.now().year + 1):
    '''
    This pipeline will extract ATP tour match data from Jeff Sackmann's Github repo of annual tour csv's 
    and consolodate it into a common database.

    There is data available from 1968 up to the present day by default.

    Users can specify the start_year and end_year kw-args in the run_pipeline() function call. 
    '''
    
    # Create a list of file locations
    tour_files = []

    for i in range(start_year, end_year):
        url_base = r'https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_'
        url_end = '{}.csv'.format(i)
        tour_files.append(url_base + url_end)

    # Extract files using list
    df_tour = pd.read_csv(tour_files[0], parse_dates=['tourney_date'])

    for i in tour_files[1:]:
        df_iter = pd.read_csv(i, parse_dates=['tourney_date'])
        df_tour = pd.concat([df_tour, df_iter])
    
    # Set up mapping groups
    map_tourney_level = {'A': 1, 'M': 2, 'F': 3, 'G': 4, 'D':0}
    map_round = {'R128': 1, 'R64': 2, 'R32': 3, 'R16': 4, 'QF': 5, 'SF': 6, 'F': 7, 'BR': 0, 'RR':5}
    null_map = {
        'London Olympics': 0, 'Rio Olympics': 0, 'Tokyo Olympics': 0, 
        'ATP Next Gen Finals': 0,'Us Open': 2000, 'Cagliari': 250, 'Marbella': 250}
    
    events_url = 'https://raw.githubusercontent.com/sciDelta/streamlit-atp-stats/main/data/atp_event_points.csv'
    events_map = pd.read_csv(events_url, index_col = 0).set_index('atp_event', drop=True)['atp_points'].to_dict()
    events_map.update(null_map)

    # Map objects to integers
    df_tour['tourney_level'] = df_tour['tourney_level'].map(map_tourney_level)
    df_tour['round_no'] = df_tour['round'].map(map_round)
    df_tour['atp_points'] = df_tour['tourney_name'].map(events_map)
    df_tour['atp_points'] = [0 if pd.isna(i) else i for i in df_tour['atp_points']]

    # Map ranking points gained from match
    round_points_win = {
        2000: {7:800, 6:480, 5:360, 4:180, 3:90, 2: 45, 1:45},
        1500: {7:500, 6:400, 5:200},
        1000: {7:400, 6:240, 5:180, 4:90, 3:45, 2: 45, 1: 0},
        500: {7:200, 6:120, 5:90, 4:45, 3:25, 2: 20},
        250: {7:100, 6:60, 5:45, 4:25, 3:15, 2: 5}}
    
    round_points_lose = {
        2000: {7:480, 6:360, 5:180, 4:90, 3:45, 2: 35, 1:10},
        1500: {7:400, 6:0, 5:0},
        1000: {7:240, 6:180, 5:90, 4:45, 3: 35, 2:10, 1: 0},
        500: {7:120, 6:90, 5:45, 4:25, 3: 20, 2:0},
        250: {7:60, 6:45, 5:25, 4:15, 3: 5, 2:0}}

    rounds = df_tour['round_no'].to_numpy()
    points = df_tour['atp_points'].to_numpy()
    
    df_tour['points_winner'] = [
        0 if points[i] == 0 else 0 if pd.isna(points[i]) 
        else 0 if rounds[i] == 0 else 0 if pd.isna(rounds[i])
        else round_points_win[points[i]][rounds[i]] for i in range(len(rounds))]
    
    df_tour['points_loser'] = [
        0 if points[i] == 0 else 0 if pd.isna(points[i]) 
        else 0 if rounds[i] == 0 else 0 if pd.isna(rounds[i])
        else round_points_lose[points[i]][rounds[i]] for i in range(len(rounds))]

    df_tour = df_tour.reset_index(drop = True)

    # Add number of sets played per match
    df_tour['sets_played'] = [0 if type(i) is float else len(i)-1 if i[-1] == 'RET' else len(i) for i in df_tour['score'].str.split(' ')]

    # Remove unrequired / unuseful columns and return data frame with a reset index
    drop_cols = ['winner_entry', 'winner_seed', 'loser_entry', 'loser_seed']
    return df_tour.reset_index(drop = True).drop(columns = drop_cols)

### Run Pipeline ###
if __name__ == '__main__':
    # Check most recent entry in DB
    database = pd.read_csv(r'../data/ATP_Tour.csv', index_col=0, parse_dates=['tourney_date'])
    last_database_update = database['tourney_date'].max()
 
    # Run pipeline extract from start of year of last db update
    new_data = run_pipeline(start_year=last_database_update.year)

    # Filter & load new entries to DB
    new_data = new_data[new_data['tourney_date'] > last_database_update]
    
    database_updated = pd.concat([database, new_data], axis=0).sort_values(by='tourney_date').reset_index(drop=True)
    database_updated.to_csv(r'../data/ATP_Tour.csv')