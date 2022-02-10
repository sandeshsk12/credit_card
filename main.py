from Database_operation.cassandra_data_op import cassandra_ops
import pandas as pd
from File_operations.file_ops import file_operations
from train_valid import train_validation
from train_model import trainmodel

a=train_validation()
a.train_validation()
b=trainmodel()
b.train_model()