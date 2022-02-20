import pandas as pd
from application_logging.logger import App_Logger
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import pickle


class trainer():
    """
                Class Name: trainer
                Description: This method is used to train a model 

                Written by: Sandesh
                Version :1
                Revisions : None
                """

    def __init__(self):

        self.logger = App_Logger()
        self.log_file = open('Logs/training_log.txt', 'a+')

    def data_splitter(self, features, target):
        """
                Method Name: data_splitter
                Description: This method is used to split the dataframe into testing and training parts
                Input: Method expects two inputs. 
                        1. dataframe consisting of independant variables
                        2. Dataframe consisting of dependant variables
                Output: Returns a tuple of 4 arrays .X_train,X_test,y_train,y_test 
                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(self.log_file, "Entered data splitter function")
        try:
            X_train, X_test, y_train, y_test = train_test_split(
                features, target, test_size=0.2, random_state=10)
            self.logger.log(
                self.log_file, "Successfully split the data into 2 parts")
            return X_train, X_test, y_train, y_test
        except Exception as e:
            self.logger.log(
                self.log_file, "Could not split. The error was : %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Finished splitting into testing and training")
        return None

    def trainer(self, X_train, y_train):
        """
                Method Name: trainer
                Description: This method is used to train and build a model on the train dataset
                Input: Expects two inputs 
                        1. Dataframe consisting of training independant variable data
                        2. Dataframe consisting of training dependant vraiable data
                Output: object of model class 
                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(self.log_file, "Start model training")
        try:
            model = LogisticRegression()
            model.fit(X_train, y_train)
            self.logger.log(self.log_file, "Successfully trained a model")
            self.save_model(model)
            return model
        except Exception as e:
            self.logger.log(
                self.log_file, "Could not split. The error was : %s" % e)
        finally:
            self.logger.log(self.log_file, "Finished Training")
        return None

    def save_model(self, model):
        """
                Method Name: save_model
                Description: This method is used to save the model.
                Input: Model object
                Output: object of model class 
                Written by: Sandesh
                Version :1
                Revisions : None
        """
        self.logger.log(self.log_file, "Saving model")
        try:
            with open('models/trained_model', 'wb') as f:
                pickle.dump(model, f)
            self.logger.log(self.log_file, "Successfully saved model")
            return model
        except Exception as e:
            self.logger.log(
                self.log_file, "Could not the model. The error was : %s" % e)
        finally:
            self.logger.log(self.log_file, "Finished saving operation")
        return None
