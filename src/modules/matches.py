### Tour Match Data Pipeline ###
import datetime as dt, pandas as pd, requests
    
def TourPipeline(start_year = 1968, end_year = dt.datetime.now().year + 1, tour: str = 'ATP'):
    '''
    This pipeline will extract tour match data from Jeff Sackmann's Github repo of annual tour csv's 
    and consolodate it into a common database.

    There is data available from 1968 up to the present day by default.

    Users can specify the start_year and end_year kw-args in the run_pipeline() function call. 
    '''
    # Create a list of file locations
    tour_files = []

    for i in range(start_year, end_year):
        if tour.upper() == 'ATP':
            url_base = r'https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_'
        elif tour.upper() == 'WTA':
            url_base = r'https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_'
        else:
            raise ValueError('Select a tour : "ATP" or "WTA"')
        
        url_end = '{}.csv'.format(i)
        tour_files.append(url_base + url_end)
    
    # Extract files using list
    for n, file in enumerate(tour_files):
        if requests.get(file).status_code == 200:
            df_iter = pd.read_csv(file, parse_dates=['tourney_date'])
            
            if n == 0:
                df_tour = df_iter
            else:
                df_tour = pd.concat([df_tour, df_iter])
    
    # Set up mapping groups
    map_tourney_level = {'A': 1, 'M': 2, 'F': 3, 'G': 4, 'D':0}
    map_round = {'R128': 1, 'R64': 2, 'R32': 3, 'R16': 4, 'QF': 5, 'SF': 6, 'F': 7, 'BR': 0, 'RR':5}
    null_map = {
        'London Olympics': 0, 'Rio Olympics': 0, 'Tokyo Olympics': 0, 
        'ATP Next Gen Finals': 0,'Us Open': 2000, 'Cagliari': 250, 'Marbella': 250}
    
    events_url = 'data/events.csv'
    events_map = pd.read_csv(events_url, index_col = 0)['tour_points'].to_dict()
    events_map.update(null_map)

    # Map objects to integers
    df_tour['tourney_level'] = df_tour['tourney_level'].map(map_tourney_level)
    df_tour['round_no'] = df_tour['round'].map(map_round)
    df_tour['tour_points'] = df_tour['tourney_name'].map(events_map)
    df_tour['tour_points'] = [0 if pd.isna(i) else i for i in df_tour['tour_points']]

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
    points = df_tour['tour_points'].to_numpy()
    
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


if __name__ == '__main__':
    import os
    tour_key = os.getenv("TOUR_KEY")

    table_name=f'{tour_key}_matches'
    table_data=TourPipeline(start_year=2011, tour=tour_key)
    table_data.to_parquet(f"database/{table_name}.parquet")
