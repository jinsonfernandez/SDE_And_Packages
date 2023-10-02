names = ['Bulbasaur','Charmander','Squirtle']
hps = [45, 39, 44]

for i in zip(names,hps):
    print(i)
combined_zip = zip(names,hps)
zipped_list = [*combined_zip]

# =============================================The collections module===============================================
from collections import Counter
poke_type = ['Grass', 'Dark','Fire', 'Fire']
type_counts = {}
for p in poke_type:
    if p not in type_counts:
        type_counts[p] =1
    else:
        type_counts[p]+=1
print(type_counts)

                                                    # collections.Counter()
poke_type = ['Grass', 'Dark','Fire', 'Fire']

type_counts = Counter(poke_type)

#========================================================= Itertools Module ===========================================
# Combination with loop
poke_types = ['Bug','Fire','Ghost','Grass','Water']
combos = []
for x in poke_types:
    for y in poke_types:
        if x == y:
            continue
        if ((x,y) not in combos) & ((y,x) not in combos):
            combos.append((x,y))

from itertools import combinations
poke_types = ['Bug','Fire','Ghost','Grass','Water']
combos_obj = combinations(poke_types, 2)
print([*combos_obj])

#=========================================================== Sets =====================================================

#Finding uique elements
primary_types = ['Grass','Psychic','Dark','Bug', ...]
unique_type = []
for i in primary_types:
    if i not  in unique_type:
        unique_type.append(i)

unique_type = set(primary_types)

#====================================================== Eliminating Loops ==============================================
poke_stats = [
[90, 92, 75, 60],
[25, 20, 15, 90],
[65, 130, 60, 75],
...
]
totals_comp = [sum(row) for row in poke_stats]
total_map = [*map(sum, poke_stats)]

#Using numpy
avgs_np = poke_stats.mean(axis=1)

#=================================================== Pandas Optimization ==============================================
import pandas as pd
import numpy as np
baseball_df = pd.read_csv('baseball_stats.csv')
print(baseball_df.head())

def calc_win_perc(wins, games_played):
    win_perc = wins / games_played
    return np.round(win_perc,2)

# Adding win_precentage to dataframe
win_perc_list = []
for i in range(len(baseball_df)):
    row = baseball_df.iloc[i]
    wins = row['W']
    games_played = row['G']
    win_perc = calc_win_perc(wins, games_played)
    win_perc_list.append(win_perc)
baseball_df['WP'] = win_perc_list

# iterating with Iterrows
win_perc_list = []
for i,row in baseball_df.iterrows():
    wins = row['W']
    games_played = row['G']
    win_perc = calc_win_perc(wins, games_played)
    win_perc_list.append(win_perc)
baseball_df['WP'] = win_perc_list

#iterating with Itertiples
team_wins_df = pd.read_csv('team.csv')
for row_namedtuple in team_wins_df.itertuples():
    print(row_namedtuple)

#Using Iterrows
for row_tuple in team_wins_df.iterrows():
    print(row_tuple[1]['Team'])

#using Itertuples
for row_namedtuple in team_wins_df.itertuples():
    print(row_namedtuple.Team)

#================================================================== Pandas alternatove to looping
def calc_run_diff(runs_scored, runs_allowed):
    run_diff = runs_scored - runs_allowed
    return run_diff

#using Iterrows
run_diffs_iterrows = []
for i,row in baseball_df.iterrows():
    run_diff = calc_run_diff(row['RS'], row['RA'])
    run_diffs_iterrows.append(run_diff)
baseball_df['RD'] = run_diffs_iterrows

#using Apply
baseball_df.apply(lambda row: calc_run_diff(row['RS'], row['RA']), axis=1 )

# Using numpy vectorisation
wins_np = baseball_df['W'].values
run_diffs_np = baseball_df['RS'].values - baseball_df['RA'].values
baseball_df['RD'] = run_diffs_np
