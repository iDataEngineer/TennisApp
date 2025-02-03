import polars as pl

class EventData:
    def __init__(self, tour:str, slam:str, year:int):
        self.tour = tour
        self.slam = slam
        self.year = year

        self.slam_url = "ausopen" if slam.lower() == "australian open" else slam.replace(" ", "").lower()

    def get_matches(self) -> pl.DataFrame:
        try: 
            data = pl.read_csv(f"https://raw.githubusercontent.com/JeffSackmann/tennis_slam_pointbypoint/master/{self.year}-{self.slam_url}-matches.csv")
        except Exception as e:
            return pl.DataFrame()
        else:
            data = data.with_columns(
                match_num = pl.col("match_id").map_elements(lambda x: x.split("-")[2], return_dtype=pl.String),
                surface = pl.lit("clay") if self.slam.lower() == "french open" else pl.lit("grass") if self.slam.lower() == "wimbledon" else pl.lit("hard")
            )

            if self.tour.lower() == "atp":
                return data.filter( (pl.col("match_num").str.starts_with("MS")) | (pl.col("match_num").str.starts_with("1")) )
            else:
                return data.filter( (pl.col("match_num").str.starts_with("WS")) | (pl.col("match_num").str.starts_with("2")) )

    def get_points(self) -> pl.DataFrame:
        try:
            data = pl.read_csv(
            source=f"https://raw.githubusercontent.com/JeffSackmann/tennis_slam_pointbypoint/master/{self.year}-{self.slam_url}-points.csv",
                infer_schema_length=10000,
            )
        except Exception as e:
            return pl.DataFrame()
        else:
            data = data.with_columns(
                P1Score = pl.col("P1Score").map_elements(lambda x: 45 if str(x) == "AD" else int(x), return_dtype=pl.Int32),
                P2Score = pl.col("P2Score").map_elements(lambda x: 45 if str(x) == "AD" else int(x), return_dtype=pl.Int32),
                PointNumber = pl.col("PointNumber").map_elements(lambda x: 0 if str(x).startswith("0") else int(x), return_dtype=pl.Int32),
            )
            return data
    
    def get_ranks(self) -> pl.DataFrame:
        return_cols = [
            "tourney_name", "winner_name", "winner_age", "winner_rank", 
            "winner_rank_points", "loser_name", "loser_age", "loser_rank", 
            "loser_rank_points", 
        ]

        if self.tour.lower() == "atp":
            url = f"https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{self.year}.csv"
        else: 
            url = f"https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_{self.year}.csv"

        try: 
            if self.slam.lower() == "french open": # name switches to RG in rank tables
                data = pl.read_csv(url).select(return_cols).filter(pl.col("tourney_name").str.to_lowercase().eq("roland garros"))       
            else:
                data = pl.read_csv(url).select(return_cols).filter(pl.col("tourney_name").str.to_lowercase().eq(self.slam.lower()))
        except Exception as e:
            return pl.DataFrame()
        else:
            return data

    def get_results(self) -> pl.DataFrame:
        # Get data
        matches = self.get_matches()
        points = self.get_points()
        ranks = self.get_ranks()

        if matches.is_empty() or points.is_empty() or ranks.is_empty():
            return pl.DataFrame()
        
        # Process rank data into single name / age / rank cols
        w_rank = ranks.select("winner_name", "winner_age", "winner_rank", "winner_rank_points").unique().rename({
            "winner_name": "name", 
            "winner_age": "age", 
            "winner_rank": "rank", 
            "winner_rank_points": "rank_points"
        })

        l_rank = ranks.select("loser_name", "loser_age", "loser_rank", "loser_rank_points").unique().rename({
            "loser_name": "name", 
            "loser_age": "age", 
            "loser_rank": "rank", 
            "loser_rank_points": "rank_points"
        })

        ranks = pl.concat([w_rank, l_rank]).unique().with_columns(
            join_name = pl.col("name").map_elements(
                function=lambda x: " ".join([i[0] if n==0 else i for n, i in enumerate(str(x).split(" "))]),
                return_dtype=pl.String
            ),
        )

        # Create combined event table
        matches = matches.with_columns(
            round = pl.col("match_id").map_elements(lambda x: int(str(x)[-3]), pl.Int32),
            p1_join_name = pl.col("player1").map_elements(
                function=lambda x: " ".join([i[0] if n==0 else i for n, i in enumerate(str(x).split(" "))]),
                return_dtype=pl.String
            ),
            p2_join_name = pl.col("player2").map_elements(
                function=lambda x: " ".join([i[0] if n==0 else i for n, i in enumerate(str(x).split(" "))]),
                return_dtype=pl.String
            ),
        )
        
        event = matches.join(ranks, left_on="p1_join_name", right_on="join_name", suffix="_p1", how="left")
        event = event.join(ranks, left_on="p2_join_name", right_on="join_name", suffix="_p2", how="left")
        event = event.join(points, "match_id", how="left")

        # Consolidate overlapping cols        
        output_cols = [
            "match_id", "surface", "player1", "player2", "winner", "round", "age", "rank", "rank_points", "age_p2", "rank_p2", "rank_points_p2", 
            "ElapsedTime", "SetNo", "P1GamesWon", "P2GamesWon", "SetWinner", "GameNo", "GameWinner", "PointNumber", "PointWinner", "PointServer", 
            "Speed_MPH", "Speed_KMH", "Rally", "RallyCount", "ServeIndicator", "ServeNumber", "P1DistanceRun", "P2DistanceRun",
            "P1Score", "P2Score", "P1PointsWon", "P2PointsWon", "P1Ace", "P2Ace", "P1Winner", "P2Winner", "P1DoubleFault", "P2DoubleFault", "P1UnfErr", "P2UnfErr",  
        ]
        
        return event.select([i for i in output_cols if i in event.columns])

if __name__ == "__main__":
    tournaments = ["Australian Open", "French Open", "Wimbledon", "US Open"]
    years = list(range(2011, 2025))

    tour_map = [(i, j) for i in tournaments for j in years]
    for event, year in tour_map:
        res = EventData("atp", event, year).get_results()
        
        if not res.is_empty():
            res.write_parquet(f"data/events/atp-{event.replace(" ", "").lower()}-{year}.parquet")
