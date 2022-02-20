

import os
import shutil
from File_operations.name_val import validate_file_name
import shutil
import json
from File_operations.schema_reader import read_schema
import pandas as pd
import numpy as np
from application_logging.logger import App_Logger


class file_operations:
    """
        This class shall be used to perform all File operations.
        Operations that can be done are 
                check if the file names are according to the naming conventions 
                Check if the column names are as specified and if columns contain data
                Impute Null value where data is missing

        Written By: Sandesh
        Version: 1.0
        Revisions: None
        """

    def __init__(self):

        self.path = 'Training_Batch_files'
        self.good_files_path = 'Training_files_validated'
        self.bad_files_path = 'Bad_Training_files'
        self.schema = read_schema()
        self.data_dic = self.schema.read_json()
        self.log_file = open('Logs/file_operations_log.txt', 'a+')
        self.logger = App_Logger()
        self.file_name = 'UCI_Credit_Card.csv'
        self.bad_file_log = open('Logs/bad_file_logs.txt')

    def break_file_into_smaller_files(self):
        """
                Method Name: break_file_into_smaller_files
                Description: Divides the main training or prediction file into files of 1000 records for easy database insertion
                Output: None

                Written by: Sandesh
                Version :1
                Revisions : None
        """
        date = 28011960
        time = 120210
        i = 0
        faulty_record = True
        self.logger.log(
            self.log_file, "Attempting to Break the complete csv file into chunks")

        try:
            data = pd.read_csv(self.file_name)

            for j in range(0, 31):
                new = data.iloc[i:i+1001]
                name = "creditCardFraud_" + \
                    str(date) + '_' + str(time) + ".csv"
                new.to_csv(self.path+'/'+name, index=None, header=True)
                i += 1000
                date += 1
                time += 1

                if i > len(data):
                    break

                # Below code is meant to forcefully create a bad file just for emulation purpose
                while faulty_record == True:
                    new.to_csv(self.path+'/' +
                               name[:-4]+'faulty.csv', index=None, header=True)
                    faulty_record = False
        except Exception as e:

            self.logger.log(
                self.log_file, "Could not break down the csv file %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Completed the process of dividing the main file")
        return None

    def check_filename_and_move(self):
        """
                Method Name: check_filename_and_move
                Description: Reads the filename and checks if it is according to the naming convention specified. Move files with correct name to good folder and 
                            move files with wrong file name into archive folder
                Output: None

                Written by: Sandesh
                Version :1
                Revisions : None
        """
        self.logger.log(self.log_file, "Attempting to verify the file names")
        try:
            for file_name in sorted(os.listdir(self.path)):
                source = (os.path.join(self.path+'/'+file_name))
                good_files_dest = (os.path.join(
                    self.good_files_path+'/'+file_name))
                bad_files_dest = (os.path.join(
                    self.bad_files_path+'/'+file_name))
                if validate_file_name().check_name(file_name) == True:
                    self.logger.log(
                        self.log_file, "Verified file {}".format(file_name))
                    try:
                        shutil.copy(source, good_files_dest)
                    except:
                        self.logger.log(
                            self.log_file, "Could not verify the file name. The error is: %s" % e)
                        self.logger.log()

                else:
                    shutil.copy(source, bad_files_dest)
                    self.logger.log(self.log_file, "Moving {} to {}".format(
                        source, bad_files_dest))
                    self.logger.log(
                        self.log_file, "Bad file identified{}".format(file_name))
                    pass
        except Exception as e:

            self.logger.log(
                self.log_file, "Could not verify the file name. The error is: %s" % e)
        self.logger.log(
            self.log_file, "Completed the process of checking the file names")

        return None

    def check_file_contents(self):
        """
                Method Name: check_file_contents
                Description: Reads the file and checks if the entire column is empty or not. Move files with empty columns to bad folder
                Output: None

                Written by: Sandesh
                Version :1
                Revisions : None
        """
        self.logger.log(self.log_file, "Attempting to check file contents")
        try:

            for file_name in sorted(os.listdir(self.good_files_path)):
                data = pd.read_csv(os.path.join(
                    self.good_files_path+'/'+file_name))
                good_files_dest = (os.path.join(
                    self.good_files_path+'/'+file_name))
                bad_files_dest = (os.path.join(
                    self.bad_files_path+'/'+file_name))
                self.logger.log(
                    self.log_file, "verifying {}".format(file_name))
                for col in data.columns:
                    #self.logger.log(self.log_file,"{},col{}, count{}".format(file_name,col,data[col].count()))
                    if data[col].count() == 0:

                        self.logger.log(
                            self.log_file, "File with entire column missing found {}".format(file_name))
                        try:
                            shutil.move(good_files_dest, bad_files_dest)
                            self.logger.log(
                                self.log_file, "Moving bad file {} to bad folder".format(file_name))
                        except Exception as e:
                            self.logger.log(
                                self.log_file, "Could not move the bad file. The error is: %s" % e)
                            self.logger.log(
                                self.bad_file_log, "filename {}, reject reason{}".format(file_name, e))
                    else:
                        pass
        except Exception as e:

            self.logger.log(
                self.log_file, "Could not check the file contents. The error is: %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Completed the process of checking the file contents")
        return None

    def impute_null(self):
        """
                Method Name: impute_null
                Description: Reads every column in the file and checks if there are any missing values. If present, it is filled with NULL value 
                Output: None

                Written by: Sandesh
                Version :1
                Revisions : None
        """
        self.logger.log(self.log_file, "Attempting to impute Null values")
        try:
            for file_name in sorted(os.listdir(self.good_files_path)):
                data = pd.read_csv(os.path.join(
                    self.good_files_path+'/'+file_name))
                good_files_dest = (os.path.join(
                    self.good_files_path+'/'+file_name))
                for col in data.columns:
                    if data[col].isna().sum() > 0:
                        self.logger.log(
                            self.log_file, "missing value found at filname:{}, column:{}".format(file_name, col))
                        data[col].fillna(np.nan, inplace=True)
                        self.logger.log(
                            self.log_file, "Imputing values for {}".format(file_name))
                data.to_csv(good_files_dest)

        except Exception as e:

            self.logger.log(
                self.log_file, "Could not check the file contents. The error is: %s" % e)
        finally:
            self.logger.log(
                self.log_file, "Completed the process of checking the file contents")
        return None
