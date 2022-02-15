from model_training_scripts.training_preprocessor import preprocessor
from application_logging.logger import App_Logger
from model_training_scripts.training import trainer


class trainmodel():
    """
    """

    def __init__(self):
        self.log_file = open('Logs/training_main.txt', 'a+')
        self.preprocessor_operator = preprocessor()
        self.training_opertaor = trainer()
        self.logger = App_Logger()

    def train_model(self):
        """
        """
        try:
            self.logger.log(self.log_file, "Preprocessing files for training")
            self.logger.log(self.log_file, "Loading data from csv file")
            data = self.preprocessor_operator.load_data()
            self.logger.log(
                self.log_file, "Successfully read data from main csv file")

            self.logger.log(
                self.log_file, "Checking if column names have empty spaces ")
            data = self.preprocessor_operator.check_empty_spaces_in_col_names(
                data)
            self.logger.log(
                self.log_file, "Successfully checked for blanks and spaces in column names")

            self.logger.log(
                self.log_file, "Seperating data into features and target variable")
            features, target = self.preprocessor_operator.seperate_target_features(
                data)
            self.logger.log(
                self.log_file, "Successfully split the dataset into features and target")

            self.logger.log(
                self.log_file, "Checking for missing values in feature dataset")
            missing_Y_N, total_number_missing, cols_with_missing_values = self.preprocessor_operator.check_missing(
                features)
            self.logger.log(
                self.log_file, "Checking for missing values complete")

            self.logger.log(
                self.log_file, "imputing values where data is missing")
            features = self.preprocessor_operator.impute_missing_values(
                features, cols_with_missing_values)
            self.logger.log(
                self.log_file, "Successfully imputed data where missing")

            self.logger.log(
                self.log_file, "Checking for columns without deviations and dropping them")
            cols_without_deviation = self.preprocessor_operator.cols_without_deviation(
                features)
            features = self.preprocessor_operator.drop_cols_without_deviation(
                features, cols_without_deviation)
            self.logger.log(
                self.log_file, "Successfully dropped columns that had very highly imbalanced dat")

            self.logger.log(self.log_file, "Scaling numerical values")
            features = self.preprocessor_operator.scale_numerical_cols(
                features)
            self.logger.log(self.log_file, "Scaling complete")

            self.logger.log(
                self.log_file, "Checking for data imbalance and handling it")
            features, target = self.preprocessor_operator.handle_imbalanced_dataset(
                features, target)
            self.logger.log(self.log_file, "Handled imbalanced data")

            # Model training starts here
            self.logger.log(
                self.log_file, "Splitting data into test and train dataset")
            X_train, X_test, y_train, y_test = self.training_opertaor.data_splitter(
                features, target)
            self.logger.log(
                self.log_file, "Successfully split data into test and train")

            self.logger.log(
                self.log_file, "Training a logistic regression model")
            model = self.training_opertaor.trainer(X_train, y_train)
            self.logger.log(self.log_file, "Model trained")

            self.logger.log(self.log_file, "Yet to save model")
            self.training_opertaor.save_model(model)
            self.logger.log(self.log_file, "Model saved")
        except:
            raise Exception
