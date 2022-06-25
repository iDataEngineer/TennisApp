# Chart functons for app
import matplotlib.pyplot as plt, seaborn as sns
sns.set()

# Plotting function
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

