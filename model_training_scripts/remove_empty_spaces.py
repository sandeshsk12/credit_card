from application_logging.logger import App_Logger
import pandas as pd

class remove_empty_spaces():
    """
                Class Name: remove_empty_spaces
                Description: This method is used to remove the empty spaces in the column name if any

                Written by: Sandesh
                Version :1
                Revisions : None
                """
    def __init__(self):
        
        self.logger = App_Logger()
        self.log_file=open('Logs/training_log.txt','a+')


    def check_empty_spaces_in_col_names(self,data):
        """
                Method Name: check_empty_spaces_in_col_names
                Description:This method is used to remove the empty spaces in the column name if any

                Written by: Sandesh
                Version :1
                Revisions : None
                """
        self.logger.log(self.log_file,"Checking for empty spaces in colum  names operation")
        new_col_names=[]
        try:
            for col_name in data.columns:
                col_name=col_name.strip(' ')
                new_col_names.append(col_name)
            
            self.logger.log(self.log_file,"Successfully checked and removed empty spaces")
            return data[new_col_names]
        except Exception as e:
            self.logger.log(self.log_file,"Failed to read file %s" %e)
        return None
        