from sklearn.metrics import confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sn
from sklearn.metrics import confusion_matrix
import pickle
import pandas as pd
from application_logging.logger import App_Logger


with open('models/trained_model', 'rb') as f:
    model = pickle.load(f)

data = pd.read_csv('Prediction_files/UCI_Credit_Card.csv')
data2 = pd.read_csv('UCI_Credit_Card.csv')

y = []
for i in range(len(data)):
    y.append(model.predict([data.iloc[i, 1:].to_list()])[0])

data['defaulted'] = y
print(data[:4])


cm = confusion_matrix(data2.iloc[:, -1], y)
sn.heatmap(cm, annot=True)
plt.xlabel("predicted")
plt.ylabel("Truth")


# printing classifcation report
print(classification_report(data2.iloc[:, -1], y))
