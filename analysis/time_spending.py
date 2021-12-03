import pandas as pd

def summary_time_use():
    path_to_file = 'data/annotations_jodie.log'
    df = pd.read_csv(path_to_file, header=None)
    df[df['']]