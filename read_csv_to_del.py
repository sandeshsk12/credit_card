import pandas as pd

def read_file(path):
    data=pd.read_csv('/mnt/work/data_science/ineuron/ccdp/UCI_Credit_Card.csv')
    return data

data=pd.read_csv('/mnt/work/data_science/ineuron/ccdp/UCI_Credit_Card.csv')
print(data.columns)