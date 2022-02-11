from pred_valid import pred_validation

import os
import shutil
from File_operations.name_val import validate_file_name
import shutil
import json
from File_operations.schema_reader import read_schema
import pandas as pd
import numpy as np
from application_logging.logger import App_Logger
from File_operations.file_ops import file_operations
from Database_operation.cassandra_data_op import cassandra_ops

file_operator=file_operations()
#database_operator=cassandra_ops()

file_operator.path='Prediction_Batch_files'
file_operator.good_files_path='Prediction_files_validated'
file_operator.bad_files_path='Bad_Prediction_files'
file_operator.schema=read_schema('schema_prediction.json')
file_operator.data_dic=file_operator.schema.read_json()
file_operator.log_file=open('Logs/file_operations_log.txt','a+')
file_operator.logger = App_Logger()
file_operator.file_name='UCI_Credit_Card.csv'


file_operator.break_file_into_smaller_files()
file_operator.check_filename_and_move()
file_operator.check_file_contents()
file_operator.impute_null()



database_operator=cassandra_ops()
database_operator.path = 'Prediction_files_validated/'
database_operator.badFilePath = "Bad_Prediction_files/"
database_operator.goodFilePath = "Prediction_files_validated/"
database_operator.logger = App_Logger()
database_operator.file=open('Logs/database_log.txt','a+')

database_operator.cassandra_connection()
database_operator.create_prediction_table()
database_operator.insert_values_into_prediction_database()