import pickle
import pandas as pd
from application_logging.logger import App_Logger


class predict():
    """
                Class Name: predict
                Description: This method is used to predict the outcome given the features

                Written by: Sandesh
                Version :1
                Revisions : None
                """

    def __init__(self):
        self.model_path = 'models/trained_model'
        self.logger = App_Logger()
        self.file_to_be_predicted = 'prediction_files_from_db/prediction_file.csv'
        self.log_file = open('Logs/Prediction_main.txt', 'a+')

    def predict(self):

        self.logger.log(self.log_file, "Start of prediction on data ")
        try:
            with open(self.model_path, 'rb') as f:
                model = pickle.load(f)
            self.logger.log(self.log_file, "Successfully loaded model")
            data = pd.read_csv(self.file_to_be_predicted)
            self.logger.log(self.log_file, "Predicting values")
            y = []
            for i in range(len(data)):
                y.append(model.predict([data.iloc[i, 1:].to_list()])[0])

            data['defaulted'] = y
            data.to_csv('predicted_files/predicted_file.csv')

        except Exception as e:
            self.logger.log(
                self.log_file, "Failed to predict .The error is {}".format(e))
            raise e


"""



with open('models/trained_model','rb') as f:
    model=pickle.load(f)

data=pd.read_csv('Prediction_files/UCI_Credit_Card.csv')
data2=pd.read_csv('UCI_Credit_Card.csv')

y=[]
for i in range(len(data)):
    y.append(model.predict([data.iloc[i,1:].to_list()])[0])

data['defaulted']=y
print(data[:4])


from sklearn.metrics import confusion_matrix

cm= confusion_matrix(data2.iloc[:,-1],y)
import seaborn as sn
import matplotlib.pyplot as plt
sn.heatmap(cm,annot=True)
plt.xlabel("predicted")
plt.ylabel("Truth")


#printing classifcation report 
from sklearn.metrics import confusion_matrix , classification_report
print(classification_report(data2.iloc[:,-1],y))
"""
