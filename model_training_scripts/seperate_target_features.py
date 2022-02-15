
from application_logging.logger import App_Logger
import pandas as pd


class seperate_target_features():
    """
                Class Name: seperate_target_features
                Description: This class is used to divide the dataset into features and target

                Written by: Sandesh
                Version :1
                Revisions : None
                """

    def __init__(self):

        self.logger = App_Logger()
        self.log_file = open('Logs/training_log.txt', 'a+')

    def seperate_target_features(self, data, target_variable='default_payment_next_month'):
        """
                Method Name: seperate_target_features
                Description:This method is used to divide the dataset into features and target
                Output: Retruns two dataframes. The first dataframe is the features dataframe
                        The second dataframe consists of the target variable

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(
            self.log_file, "Attempting to split data into seperate dataframes containing features and target")

        try:
            features = data.drop(target_variable, axis=1)
            self.logger.log(self.log_file, "Created features dataframe")
            target = data[[target_variable]]
            self.logger.log(self.log_file, "Created target dataframe")

            return features, target
        except Exception as e:
            self.logger.log(
                self.log_file, "Failed to split the dataframe into features and target %s" % e)
        return None
