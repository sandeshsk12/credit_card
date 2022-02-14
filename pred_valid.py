from concurrent.futures import process
from File_operations.file_ops import file_operations
from application_logging.logger import App_Logger
from Database_operation.cassandra_data_op import cassandra_ops
from File_operations.schema_reader import read_schema

class pred_validation():
    """
    
    """
    def __init__(self):
        self.log_file=open('Logs/Prediction_main.txt','a+')
        self.logger = App_Logger()
        
        self.file_operator=file_operations()
        #database_operator=cassandra_ops()

        self.file_operator.path='Prediction_Batch_files'
        self.file_operator.good_files_path='Prediction_files_validated'
        self.file_operator.bad_files_path='Bad_Prediction_files'
        self.file_operator.schema=read_schema('schema_prediction.json')
        self.file_operator.data_dic=self.file_operator.schema.read_json()
        self.file_operator.log_file=open('Logs/file_operations_log.txt','a+')
        self.file_operator.logger = App_Logger()
        self.file_operator.file_name='UCI_Credit_Card.csv'
        

        self.database_operator=cassandra_ops()
        self.database_operator.path = 'Prediction_files_validated/'
        self.database_operator.badFilePath = "Bad_Prediction_files/"
        self.database_operator.goodFilePath = "Prediction_files_validated/"
        self.database_operator.logger = App_Logger()
        self.database_operator.file=open('Logs/database_log.txt','a+')
        self.database_operator.output_folder='prediction_files_from_db'

        
    
    def pred_validation(self):
        """
        """
        #Start pred
        self.logger.log(self.log_file,"Start of prediction operation")
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


            
            #Database operations doesnt work yet
            


            self.logger.log(self.log_file,"Database operations begins here")
            self.logger.log(self.log_file,"Establishing connection with cassandra database")
            self.database_operator.cassandra_connection()
            self.logger.log(self.log_file,"Successfully connected to the database")
            
            self.logger.log(self.log_file,"Creating table in cassandra database")
            self.database_operator.create_prediction_table()
            self.logger.log(self.log_file,"Successfully created a table in the database")

            
            self.logger.log(self.log_file,"Inserting values into table in cassandra database")
            #self.database_operator.insert_values_into_prediction_database()
            self.logger.log(self.log_file,"Successfully inserted data into table in the database")

            
            self.logger.log(self.log_file,"Exporting contents in cassandra database into a csv")
            self.database_operator.db_to_csv(process='prediction')
            self.logger.log(self.log_file,"Successfully exported data in the database")

            
            self.logger.log(self.log_file,"closing cassandra database connection")
            self.database_operator.close_cassandra()
            self.logger.log(self.log_file,"Successfully closed session")

            self.log_file.close()
        except Exception as e:
            raise e 



