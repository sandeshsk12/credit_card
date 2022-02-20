from syslog import LOG_INFO
import pandas as pd
from application_logging.logger import App_Logger
from sklearn.impute import KNNImputer
import statistics
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler


class preprocessor():
    """
                Class Name: preprocessor
                Description: This method is used to preprocess or clean the data before data is used for training

                Written by: Sandesh
                Version :1
                Revisions : None
                """

    def __init__(self):

        self.logger = App_Logger()
        self.log_file = open('Logs/training_log.txt', 'a+')

    def load_data(self):
        """
                Method Name: load_data
                Description: TThis method is used to read the csv file that is generated from the cassandra database
                Output: training data in the form of a dataframe 

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.path = 'training_file_from_db/training_file.csv'
        self.logger.log(self.log_file, "Starting reading operation")
        try:
            self.data = pd.read_csv(self.path)
            self.logger.log(self.log_file, "Successfully read file")
            return self.data
        except Exception as e:
            self.logger.log(self.log_file, "Failed to read file %s" % e)
        finally:
            self.logger.log(self.log_file, "Finished reading operation")
        return None

    def check_empty_spaces_in_col_names(self, data):
        """
                Method Name: check_empty_spaces_in_col_names
                Description: This method is used to remove the empty spaces in the column name if any
                output: dataframe with column names that dont have spaces

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(
            self.log_file, "Checking for empty spaces in colum  names operation")
        new_col_names = []
        try:
            for col_name in data.columns:
                col_name = col_name.strip(' ')
                new_col_names.append(col_name)

            self.logger.log(
                self.log_file, "Successfully checked and removed empty spaces")
            return data[new_col_names]
        except Exception as e:
            self.logger.log(self.log_file, "Failed to read file %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Finished checking for empty spaces in column names")
        return None

    def seperate_target_features(self, data, target_variable='Defaulted'):
        """
                Method Name: seperate_target_features
                Description:This method is used to divide the dataset into features and target
                Input: String describing the target variable
                Output: Retruns two dataframes. The first dataframe is the features dataframe
                        The second dataframe consists of the target variable

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(
            self.log_file, "Attempting to split data into seperate dataframes containing features and target")
        print(data.columns)
        print(len(data.columns))
        try:
            features = data.drop(target_variable, axis=1)
            self.logger.log(self.log_file, "Created features dataframe")
            target = data[[target_variable]]
            self.logger.log(self.log_file, "Created target dataframe")

            return features, target
        except Exception as e:
            self.logger.log(
                self.log_file, "Failed to split the dataframe into features and target %s" % e)
        finally:
            self.logger.log(
                self.log_file, "finished splitting data into features and target")
        return None

    def check_missing(self, data):
        """
                Method Name: check_missing
                Description: This method is used to check if the dataframe has any missing values.
                Input: dataframe
                Output: Returns a tuple of Boolean,Integer and array. 
                        Boolean-True if missing values are present and False if there are no missing values
                        Integer - The total number of missing values in the entire dataframe
                        array- List of columns with missing values

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(
            self.log_file, "Checking for missing values in dataframes")
        self.number_missing = 0
        self.total_number_missing = 0
        self.cols_with_missing_values = []
        try:
            for col_name in data.columns:
                self.number_missing = 0
                self.number_missing = data[col_name].isna().sum()
                self.total_number_missing = self.total_number_missing+self.number_missing
                if self.number_missing > 0:
                    self.logger.log(
                        self.log_file, "Found missing values in column: %s" % col_name)
                    self.cols_with_missing_values.append(col_name)
                else:
                    pass
            if self.total_number_missing > 0:
                self.logger.log(
                    self.log_file, "Found a total of %s missing values" % self.total_number_missing)
                return True, self.total_number_missing, self.cols_with_missing_values
            else:
                self.logger.log(self.log_file, "No missing values found!!")
                return False, self.total_number_missing, self.cols_with_missing_values

        except Exception as e:
            self.logger.log(self.log_file, "Failed to read file %s" % e)
        finally:
            self.logger.log(
                self.log_file, "finished checking for missing values")
        return None

    def cols_without_deviation(self, data):
        """
                                        Method Name: cols_without_deviation
                                        Description: This method identifies columns that are highly skewed or imbalanced
                                        Input : Dataframe
                                        Output: Returns list of columns with very low standard deviation
                                        On Failure: Raise Exception

                                        Written By: Sandesh
                                        Version: 1.0
                                        Revisions: None
        """
        self.logger.log(
            self.log_file, "Checking for columns that dont have variation")
        self.cols_without_deviation = []
        try:
            for col in data.columns:
                if (statistics.stdev(data[col])) < 0.5:
                    self.logger.log(
                        self.log_file, "column {} appears to be highly skewed". format(col))
                    self.cols_without_deviation.append(col)
                else:
                    pass
            self.logger.log(
                self.log_file, "Completed Checking for columns that dont have variation")
            return self.cols_without_deviation

        except Exception as e:
            self.logger.log(self.log_file, "Failed to read file %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Finished checking for columns that dont have variation in data")
        return None

    def drop_cols_without_deviation(self, data, cols_without_deviation):
        """
                                        Method Name: cols_without_deviation
                                        Description: This method drops columns that are highly skewed or imbalanced
                                        Input: DataFrame
                                        Output: Returns dataframe without columns having low standard deviation
                                        On Failure: Raise Exception

                                        Written By: Sandesh
                                        Version: 1.0
                                        Revisions: None
        """
        self.logger.log(
            self.log_file, "dropping columns that dont have variation")

        try:

            data.drop(cols_without_deviation, axis=1, inplace=True)
            self.logger.log(
                self.log_file, "dropped columns that dont have variation")
            return data

        except Exception as e:
            self.logger.log(self.log_file, "Failed to read file %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Completed the process of dropping columns with low deviation")
        return None

    def impute_missing_values(self, data, cols_with_missing_values):
        """
                                        Method Name: impute_missing_values
                                        Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                                        Input: expecting two inputs
                                        1. Dataframe
                                        2. Columns with missing values

                                        Output: A Dataframe which has all the missing values imputed.
                                        On Failure: Raise Exception

                                        Written By: iNeuron Intelligence
                                        Version: 1.0
                                        Revisions: None
                     """
        self.logger.log(
            self.log_file, 'Entered the impute_missing_values method of the Preprocessor class')
        self.data = data
        self.cols_with_missing_values = cols_with_missing_values
        try:
            self.imputer = KNNImputer()
            for col in self.cols_with_missing_values:
                self.data[col] = self.imputer.fit_transform(self.data[col])
            self.logger.log(
                self.log_file, 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.data
        except Exception as e:
            self.logger.log(
                self.log_file, 'Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger.log(
                self.log_file, 'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
        finally:
            self.logger.log(
                self.log_file, "Finished the process of imputing null values")
            raise Exception()

    def scale_numerical_cols(self, data):
        """
                                        Method Name: scale_numerical_cols
                                        Description: This method scales the numerical values using a standard scaler
                                        Input: dataframe
                                        On Failure: Raise Exception
                                        output: Scaled dataframe

                                        Written By: Sandesh
                                        Version: 1.0
                                        Revisions: None
                     """
        self.logger.log(self.log_file, "Scaling numerical columns")
        try:
            scaler = StandardScaler()
            data = scaler.fit_transform(data)
            self.logger.log(self.log_file, "Finished Scaling")
            return pd.DataFrame(data)
        except Exception as e:

            self.logger.log(
                self.log_file, "Could not scale. The error was : %s" % e)
        finally:
            self.logger.log(self.log_file, "Finished scaling data")
        return None

    def handle_imbalanced_dataset(self, x, y):
        """
        Method Name: handle_imbalanced_dataset
        Description: This method handles the imbalanced dataset to make it a balanced one.
        Input: Expects two inputs.
                1. Dataframe consisting of independant variables
                2. DataFrame consisting of dependent variables
        Output: new balanced feature and target columns
        On Failure: Raise Exception

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
                                    """
        self.logger.log(self.log_file,
                        'Entered the handle_imbalanced_dataset method of the Preprocessor class')

        try:
            self.rdsmple = RandomOverSampler()
            y = ((y.to_numpy()).reshape(-1))
            self.x_sampled, self.y_sampled = self.rdsmple.fit_resample(x, y)
            self.logger.log(self.log_file,
                            'dataset balancing successful. Exited the handle_imbalanced_dataset method of the Preprocessor class')
            return self.x_sampled, self.y_sampled

        except Exception as e:
            self.logger.log(self.log_file,
                            'Exception occured in handle_imbalanced_dataset method of the Preprocessor class. Exception message:  ' + str(
                                e))
            self.logger.log(self.log_file,
                            'dataset balancing Failed. Exited the handle_imbalanced_dataset method of the Preprocessor class')
            raise Exception()
        finally:
            self.logger.log(self.log_file, "Finished balancing datasets")
