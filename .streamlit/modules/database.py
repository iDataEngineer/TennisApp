# Database utils 
import duckdb, pandas as pd

class DatabaseManager:
    '''Execute a query on a duckDB database'''
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = duckdb.connect(database=self.db_path, read_only=False)
    
    def create_db(self, db_name: str, db_path: str | None = None, read_only: bool | None = None, in_memory: bool | None = None ):
        '''Create a new DuckDB database with db_name, saved to db_path, or create an in-memory temporary db'''
        if in_memory:
            setattr(self, db_name, duckdb.connect(database=':memory:'))

        else:
            
            if read_only == None:
                setattr(self, db_name, duckdb.connect(database=db_path))
            
            else:
                setattr(self, db_name, duckdb.connect(database=db_path, read_only=read_only))

    def create_table(self, table_name: str, table_data: pd.DataFrame, ):
        '''Create a table in the DB under table_name & containing table_data'''
        self.con.execute(query=f'''CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM table_data;''')

    def execute(self, query: str):
        '''Execute a provided query on the connected DB'''
        return self.con.execute(query=query)
