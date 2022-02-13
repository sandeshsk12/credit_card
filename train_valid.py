from File_operations.file_ops import file_operations
from application_logging.logger import App_Logger
from Database_operation.cassandra_data_op import cassandra_ops

class train_validation():
    """
    
    """
    def __init__(self):
        self.log_file=open('Logs/training_main.txt','a+')
        self.logger = App_Logger()
        self.file_operator=file_operations()
        self.database_operator=cassandra_ops()
    
    def train_validation(self):
        """
        """
        #Start training
        self.logger.log(self.log_file,"Start of training operation")
        try:    
            # Break files into smaller chunks
            self.logger.log(self.log_file,"Breaking the csv file into smaller chunks")
            self.file_operator.break_file_into_smaller_files()
            self.logger.log(self.log_file,"Completed Breaking the csv file into smaller chunks")
            self.logger.log(self.log_file,"Checking for file name violation")
            self.file_operator.check_filename_and_move()
            self.logger.log(self.log_file,"Checking for file name violation complete")
            self.logger.log(self.log_file,"Checking if any column has an empty column")
            self.file_operator.check_file_contents()
            self.logger.log(self.log_file,"Files with empty columns moved into archive")
            self.logger.log(self.log_file,"Checking and imputing NULL where data is missing")
            self.file_operator.impute_null()
            self.logger.log(self.log_file,"Imputed empty spaces with NULL successfully")
            self.logger.log(self.log_file,"Checking for file name violation")
            self.file_operator.check_filename_and_move()
            self.logger.log(self.log_file,"Checking for file name violation complete")

            
            #Database operations
            self.logger.log(self.log_file,"Database operations begins here")
            self.logger.log(self.log_file,"Establishing connection with cassandra database")
            self.database_operator.cassandra_connection()
            self.logger.log(self.log_file,"Successfully connected to the database")
            
            self.logger.log(self.log_file,"Creating table in cassandra database")
            self.database_operator.create_training_table()
            self.logger.log(self.log_file,"Successfully created a table in the database")

            
            self.logger.log(self.log_file,"Inserting values into table in cassandra database")
            self.database_operator.insert_values_into_training_database()
            self.logger.log(self.log_file,"Successfully inserted data into table in the database")

            
            self.logger.log(self.log_file,"Exporting contents in cassandra database into a csv")
            self.database_operator.db_to_csv(process='training')
            self.logger.log(self.log_file,"Successfully exported data in the database")

            
            self.logger.log(self.log_file,"closing cassandra database connection")
            self.database_operator.close_cassandra()
            self.logger.log(self.log_file,"Successfully closed session")

            self.log_file.close()
        except Exception as e:
            raise e 



