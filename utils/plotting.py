# Chart functons for app
import matplotlib.pyplot as plt, seaborn as sns, pandas as pd
sns.set()

# Scatter 
def scatter_chart(data:pd.DataFrame, group_col = 'tourney_level'):
    # Group and arrange data
    groups = data.groupby(group_col)

    level_d = {'Grand Slam': 4, 'Tour Finals': 3, 'Masters 1000': 2, 'ATP 250 & 500': 1, 'Davis Cup': 0,}
    level_d_inv = {v: k for k, v in level_d.items()}

    #Build chart
    fig = plt.figure(figsize = (12,6))

    for name, group in groups:
        name = level_d_inv[name]
        plt.plot(group['tourney_date'], (group['minutes'] / 60).round(1), markersize=4, marker='o', linestyle='', label=name)

    # Plot color
    plt.rcParams['axes.facecolor']= 'gainsboro'
    plt.rcParams['savefig.facecolor']= 'gainsboro'

    # Output
    plt.legend()
    plt.ylabel('Hours')
    return fig



# Pie function
def pie_plot(selected_player, data, outcome):
    pie_fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize = (3, 4))

    # Plot pie chart
    ax.pie(x = data, 
            autopct = '%.0f%%', 
            textprops={'fontsize': 8, 'color': 'black'}, 
            pctdistance = 1.225)

    # Combine col name & player name for title
    ax.set_title(f'Matches {outcome} by {selected_player}'.title(), fontsize = 10)
    
    # Plot color
    plt.rcParams['axes.facecolor']= 'gainsboro'
    plt.rcParams['savefig.facecolor']= 'gainsboro'

    # Add legend
    pie_fig.legend(data.index, 
        loc = 'lower center', 
        prop={'size': 8}, ncol = 6, frameon = False,
        bbox_to_anchor=(0.5, 0.07))
    
    return pie_fig

