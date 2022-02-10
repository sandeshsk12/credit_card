import pandas as pd
from application_logging.logger import App_Logger

class data_loader():
    """
                Class Name: data_loader
                Description: This method is used to read the csv file that is generated from the cassandra database

                Written by: Sandesh
                Version :1
                Revisions : None
                """
    def __init__(self):
        self.path='training_file_from_db/training_file.csv'
        self.logger = App_Logger()
        self.log_file=open('Logs/training_log.txt','a+')


    def load_data(self):
        """
                Method Name: load_data
                Description: TThis method is used to read the csv file that is generated from the cassandra database
                Output: training data in the form of a dataframe 

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(self.log_file,"Starting reading operation")
        try:
            self.data=pd.read_csv(self.path)
            self.logger.log(self.log_file,"Successfully read file")
            return self.data
        except Exception as e:
            self.logger.log(self.log_file,"Failed to read file %s" %e)
        return None
        
