import pickle
import pandas as pd

with open('models/trained_model') as f:
    model=pickle.load(f)

data=pd.read_csv('Prediction_files/UCI_Credit_Card.csv')