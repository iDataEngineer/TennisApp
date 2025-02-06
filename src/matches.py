### Tour Match Data Pipeline ###
import datetime as dt, polars as pl
    
def get_tour_results(start_year = 1968, end_year = dt.datetime.now().year + 1, tour: str = "ATP"):
    """
    This pipeline will extract tour match data from Jeff Sackmann"s Github repo of annual tour csv"s 
    and consolodate it into a common database.

    There is data available from 1968 up to the present day by default.

    Users can specify the start_year and end_year kw-args in the run_pipeline() function call. 
    """
    # Create a list of file locations
    tour_files = []

    for i in range(start_year, end_year):
        if tour.upper() == "ATP":
            url_base = r"https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_"
        elif tour.upper() == "WTA":
            url_base = r"https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_"
        else:
            raise ValueError("Select a tour : 'ATP' or 'WTA'")
        
        url_end = "{}.csv".format(i)
        tour_files.append(url_base + url_end)
    
    # Extract files using list
    df_tour = pl.DataFrame()
    for n, file in enumerate(tour_files):
        try:
            df_iter = pl.read_csv(file)
        
        except Exception:
            pass
        
        else:
            df_iter = df_iter.with_columns(
                tourney_date= pl.col("tourney_date").map_elements(lambda x: dt.datetime.strptime(str(x).replace("-", ""), "%Y%m%d"), pl.Datetime)
            )
            
            if n == 0:
                df_tour = df_iter
            else:
                df_tour = pl.concat([df_tour, df_iter], how="vertical_relaxed")
    
    # Set up mapping groups
    map_tourney_level = {"A": 1, "M": 2, "F": 3, "G": 4}
    map_round = {"R128": 1, "R64": 2, "R32": 3, "R16": 4, "QF": 5, "SF": 6, "F": 7, "RR":5}
    null_map = {
        "London Olympics": 0, "Rio Olympics": 0, "Tokyo Olympics": 0, "Paris Olympics": 0,
        "ATP Next Gen Finals": 0,"Us Open": 2000, "Cagliari": 250, "Marbella": 250}
    
    events_url = "data/static/events.csv"
    events_map = pl.read_csv(events_url)
    events_map = {e : p for e in events_map.select("event").to_series().to_list() for p in events_map.select("points").to_series().to_list()}
    events_map.update(null_map)

    # Map objects to integers
    df_tour = df_tour.with_columns(
        tourney_level=pl.col("tourney_level").cast(pl.String).replace_strict(map_tourney_level, default=0),
        round_no=pl.col("round").cast(pl.String).replace_strict(map_round, default=0),
        tour_points=pl.col("tourney_name").map_elements(lambda x: 0 if str(x) not in events_map.keys() else events_map[str(x)], pl.Int64)
    )
    
    # Map ranking points gained from match
    round_points_win = {
        2000: {7:800, 6:480, 5:360, 4:180, 3:90, 2: 45, 1:45},
        1500: {7:500, 6:400, 5:200},
        1000: {7:400, 6:240, 5:180, 4:90, 3:45, 2: 45, 1: 0},
        500: {7:200, 6:120, 5:90, 4:45, 3:25, 2: 20, 1:0},
        250: {7:100, 6:60, 5:45, 4:25, 3:15, 2: 5, 1:0}}
    
    round_points_lose = {
        2000: {7:480, 6:360, 5:180, 4:90, 3:45, 2: 35, 1:10},
        1500: {7:400, 6:0, 5:0},
        1000: {7:240, 6:180, 5:90, 4:45, 3: 35, 2:10, 1: 0},
        500: {7:120, 6:90, 5:45, 4:25, 3: 20, 2:0, 1:0},
        250: {7:60, 6:45, 5:25, 4:15, 3: 5, 2:0, 1:0}}
    

    df_tour = df_tour.with_columns(
        points_winner=pl.struct("tour_points", "round_no"
        ).map_elements(
            lambda x: round_points_win[x["tour_points"]][x["round_no"]] if x["tour_points"] > 0 and x["round_no"] > 0 else 0, 
            pl.Int64
        ),
        points_loser=pl.struct("tour_points", "round_no"
        ).map_elements(
            lambda x: round_points_lose[x["tour_points"]][x["round_no"]] if x["tour_points"] > 0 and x["round_no"] > 0 else 0, 
            pl.Int64
        ),
        sets_played=pl.when(
            (pl.col("score").str.ends_with("RET")) |
            (pl.col("score").str.ends_with("W/O")) |
            (pl.col("score").str.to_lowercase().str.ends_with("walkover"))
        ).then(pl.col("score").map_elements(lambda x : len(str(x).split(" ")), pl.Int64) - 1
        ).otherwise(
            pl.when(pl.col("score").is_null()
            ).then(pl.lit(0)
            ).otherwise(pl.col("score").map_elements(lambda x : len(str(x).split(" ")), pl.Int64))
        ),
    )

    # Remove unrequired / unuseful columns and return data frame with a reset index
    drop_cols = ["winner_entry", "winner_seed", "loser_entry", "loser_seed"]
    return df_tour.drop(drop_cols)


if __name__ == "__main__":
    import os
    tour_key = os.getenv("TOUR_KEY")
    
    table_name=f"{tour_key}_matches"
    table_data=get_tour_results(start_year=2011, tour=tour_key)
    table_data.write_parquet(f"data/matches/{table_name}.parquet")

    print(table_data)
