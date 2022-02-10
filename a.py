import pandas as pd
df=pd.read_csv('UCI_Credit_Card.csv')
df.sort_values(by=['ID'],ascending=True,inplace=True)
import random
from scipy import stats

for col in df.columns:
    print(col)
    print(stats.kstest(df[col], stats.uniform(loc=0.0, scale=100.0).cdf))