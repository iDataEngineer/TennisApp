# Build tools
import pandas as pd

class Utilities:
    '''Transformation tasks for pipeline classes'''
    @staticmethod
    def points_totaliser(table: pd.DataFrame):
        table = table.reset_index(drop=True)
        set_no = table['SetNo'].to_numpy(dtype=float)
        game_no = table['GameNo'].to_numpy(dtype=float)
        set_win = table['SetWinner'].to_numpy(dtype=float)
        game_win = table['GameWinner'].to_numpy(dtype=float)
        point_win = table['PointWinner'].to_numpy(dtype=float)
        point_server = table['PointServer'].to_numpy(dtype=float) 
        serve_ind = table['ServeIndicator'].to_numpy(dtype=float)

        # Ensure SetWinner, GameWinner cols are complete using GameNo & PointWinner cols to in fill
        for i in table.index:
            # work with next row to check if new game / set - last row excl.
            if i < (len(table.index) - 1):
                j = i + 1

                # Check if game has completed and assign point winner from previous row as game winner for that row 
                if game_no[i] != game_no[j]:
                    game_win[i] = point_win[i]  
                
                if set_no[i] != set_no[j]:
                    set_win[i] = point_win[i]  
            
            # final table row - look within row only
            if i == (len(table.index) - 1):
                game_win[i] = point_win[i]
                set_win[i] = point_win[i]

        table['GameWinner'] = game_win
        table['SetWinner'] = set_win

        # around 2015 ServeNumber is used - infill back to ServeIndicator
        if 'ServeNumber' in table.columns:
            table['ServeIndicator'] = [i if i in [1, 2] else table.loc[n, 'ServeNumber'] for n, i in enumerate(table['ServeNumber'])]

        # Update serve cols
        for j in [1, 2]: 
            first_won = table[f'P{j}FirstSrvWon'].to_numpy()
            first_in = table[f'P{j}FirstSrvIn'].to_numpy()
            second_won = table[f'P{j}SecondSrvWon'].to_numpy()
            second_in = table[f'P{j}SecondSrvIn'].to_numpy()
            double_f = table[f'P{j}DoubleFault'].to_numpy()
        
            for i in table.index:
                # first serve won
                if first_won[i] not in [1,0] and (point_server[i] == j and serve_ind[i] == 1 and point_win[i] == j):
                    first_in[i] = 1
                    first_won[i] = 1
                
                # first serve in
                elif first_in[i] not in [1,0] and (point_server[i] == j and serve_ind[i] == 1):
                    first_in[i] = 1
                
                # second serve won
                if second_won[i] not in [1,0] and (point_server[i] == j and serve_ind[i] == 2 and double_f[i] != 1 and point_win[i] == j):
                    second_won[i] = 1
                    second_in[i] = 1

                # second serve in
                elif second_in[i] not in [1,0] and (point_server[i] == j and serve_ind[i] == 2 and double_f[i] != 1):
                    second_in[i] = 1
            
            table[f'P{j}FirstSrvWon'] = first_won
            table[f'P{j}FirstSrvIn'] = first_in
            table[f'P{j}SecondSrvWon'] = second_won
            table[f'P{j}SecondSrvIn'] = second_in

        return table.copy()

    @staticmethod
    def name_formatter(table: pd.DataFrame, column_list: list = ['player1', 'player2']):
        unique_names = []
        for col in column_list:
            [unique_names.append(name) for name in table[col].to_list() if name not in unique_names]
                
        # create name dictionary with key as original and val as name in form 'A. Person'
        name_dict = dict()
        for name in unique_names:
            if type(name) != str:
                name_dict[name] = name

            else:
                spliter = str(name).split(' ', maxsplit = 1)

                if len(spliter) < 2:
                    spliter = str(name).split('.', maxsplit = 1)
                
                surname = spliter[1].strip()

                # if first name is more than a single char - call first char
                if len(spliter[0]) > 1:                
                    initial = str(name).split(' ', maxsplit=1)[0][0].strip()
                    common_name = f'{initial}. {surname}'
                
                else: # call the single char
                    initial = str(name).split(' ', maxsplit=1)[0].strip()
                    common_name = f'{initial}. {surname}'

                name_dict[name] = common_name

        # map updated names into each column
        for col in column_list:
            table[col] = table[col].map(name_dict)

        return table.copy()
