from sklearn.metrics import confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sn
from sklearn.metrics import confusion_matrix
import pickle
import pandas as pd
from application_logging.logger import App_Logger


class predict_data():
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

    def predict_data(self):

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
            data.to_csv('predicted_files/predicted_file.csv', index=False)

        except Exception as e:
            self.logger.log(
                self.log_file, "Failed to predict .The error is {}".format(e))
            raise e
