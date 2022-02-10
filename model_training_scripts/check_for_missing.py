import pandas as pd
from application_logging.logger import App_Logger

class check_missing():
    """
                Class Name: check_missing
                Description: This method is used to check if the dataframe has any missing values.

                Written by: Sandesh
                Version :1
                Revisions : None
                """
    def __init__(self):
        self.path='training_file_from_db/training_file.csv'
        self.logger = App_Logger()
        self.log_file=open('Logs/training_log.txt','a+')


    def check_missing(self,data):
        """
                Method Name: check_missing
                Description: This method is used to check if the dataframe has any missing values.
                Output: Returns a tuple of Boolean,Integer and array. 
                        Boolean-True if missing values are present and False if there are no missing values
                        Integer - The total number of missing values in the entire dataframe
                        array- List of columns with missing values

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(self.log_file,"Checking for missing values in dataframes")
        self.number_missing=0
        self.total_number_missing=0
        self.cols_with_missing_values=[]
        try:
            for col_name in data.columns:
                self.number_missing=0
                self.number_missing=data[col_name].isna().sum()
                self.total_number_missing= self.total_number_missing+self.number_missing
                if self.number_missing>0:
                    self.logger.log(self.log_file,"Found missing values in column: %s" % col_name)
                    self.cols_with_missing_values.append(col_name)
                else:
                    pass
            if self.total_number_missing>0:
                self.logger.log(self.log_file,"Found a total of %s missing values" % self.total_number_missing)
                return True, self.total_number_missing,self.cols_with_missing_values
            else:
                self.logger.log(self.log_file,"No missing values found!!")
                return False,self.total_number_missing,self.cols_with_missing_values


                
            
        except Exception as e:
            self.logger.log(self.log_file,"Failed to read file %s" %e)
        return None
        
