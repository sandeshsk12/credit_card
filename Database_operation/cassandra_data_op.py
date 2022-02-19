from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
import csv
import os
import pandas as pd
from application_logging.logger import App_Logger
import shutil
from File_operations.schema_reader import read_schema


class cassandra_ops:
    """
    This class shall be used to perform cassandra operations.
    Operations that can be done are 
            create table 
            insert values into table
            show values
            drop table
            close connection
    Written By: Sandesh
    Version: 1.0
    Revisions: None
    """

    def __init__(self):
        self.path = 'Training_files_validated/'
        self.badFilePath = "Bad_Training_files/"
        self.goodFilePath = "Training_files_validated/"
        self.logger = App_Logger()
        self.file = open('Logs/database_log.txt', 'a+')
        self.output_folder = 'training_file_from_db'

    def cassandra_connection(self):
        """
        Method Name: Cassandra connection
        Description: This method is used to connect to a cloud cassandra database. 
        Output: None
        On failure : log erro but not do nothing
        Written by: Sandesh
        Version :1
        Revisions : None
        """
        self.logger.log(
            self.file, "Attempting to establish a connection between computer and cassandra database")
        try:
            cloud_config = {
                'secure_connect_bundle': 'secure-connect-ineuron.zip'
            }
            auth_provider = PlainTextAuthProvider(
                'FfdDwAlGnqNUwQSlnZHEXhZg', '-WT.2BcPIAquAyRmrPQrI6-DT3IydQ1nT,IRTkorOWI1pUsxhZNYbxJMYRqFXjg+TFpB0Iox7fkfw74Nqpo9cWSp-OgmQ57CFYJb0OCiHDr7_79ULfWcNHEN3G+4tued')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

            self.session = cluster.connect()

            self.logger.log(
                self.file, "Connected to remote cassandra database successfully")

        except Exception as e:

            self.logger.log(
                self.file, "Failed to connect to remote cassandra connection. The error is : %s" % e)

        finally:
            self.logger.log(
                self.file, "Exiting the cassandra_connection function")

        return None

    def create_training_table(self):
        """
        Method Name: create_table
        Description: This method is used to create a training table in the remote cassandra database
        Output: None
        Written by: Sandesh
        Version :1
        Revisions : None
        """
        self.logger.log(
            self.file, "Attempting to create a training database in the remote cassandra database")
        try:

            self.session.execute('create table ccdp.training_database  \
                                ( "ID" float,"LIMIT_BAL" float,"SEX" float,"EDUCATION" float, \
                                "MARRIAGE" float,"AGE" float,"PAY_0" float,"PAY_2" float, \
                                "PAY_3" float,"PAY_4" float,"PAY_5" float,"PAY_6" float, \
                                "BILL_AMT1" float,"BILL_AMT2" float,"BILL_AMT3" float, \
                                "BILL_AMT4" float,"BILL_AMT5" float,"BILL_AMT6" float, \
                                "PAY_AMT1" float,"PAY_AMT2" float,"PAY_AMT3" float, \
                                "PAY_AMT4" float,"PAY_AMT5" float,"PAY_AMT6" float, "Defaulted" float, \
                                primary key("ID"))')
            self.logger.log(self.file, "Training Table successfully created")
        except Exception as e:

            self.logger.log(
                self.file, "Training Table creation failed. The error is : %s" % e)
        finally:
            self.logger.log(
                self.file, "Exiting the create_training_table method")
        return None

    def create_prediction_table(self):
        """
        Method Name: create_table
        Description: This method is used to create a table for prediction data in the cassandra database
        Output: None
        Written by: Sandesh
        Version :1
        Revisions : None
        """
        self.logger.log(
            self.file, "Attempting to create a prediction database in the remote cassandra database")
        try:

            self.session.execute('create table ccdp.prediction_database  \
                                ( "ID" float,"LIMIT_BAL" float,"SEX" float,"EDUCATION" float, \
                                "MARRIAGE" float,"AGE" float,"PAY_0" float,"PAY_2" float, \
                                "PAY_3" float,"PAY_4" float,"PAY_5" float,"PAY_6" float, \
                                "BILL_AMT1" float,"BILL_AMT2" float,"BILL_AMT3" float, \
                                "BILL_AMT4" float,"BILL_AMT5" float,"BILL_AMT6" float, \
                                "PAY_AMT1" float,"PAY_AMT2" float,"PAY_AMT3" float, \
                                "PAY_AMT4" float,"PAY_AMT5" float,"PAY_AMT6" float, \
                                primary key("ID"))')
            self.logger.log(self.file, "Prediction Table successfully created")
        except Exception as e:

            self.logger.log(
                self.file, "Prediction Table creation failed. The error is : %s" % e)
        finally:
            self.logger.log(
                self.file, "Exiting the create prediction_table_method")
        return None

    def insert_values_into_training_database(self):
        """
                        Method Name: insert_values
                        Description: This method inserts the data in validated training files into a remote cassandra database.
                        Output: None



                        Written by: Sandesh
                        Version: 2.0
                        Revisions: None

        """
        self.logger.log(
            self.file, "Attempting to insert values into training database")
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in sorted(os.listdir(goodFilePath))]
        log_file = open('Logs/database_insertion_log.txt', 'a+')

        for file in onlyfiles:

            with open(goodFilePath+'/'+file, 'r') as csv_data:
                next(csv_data)
                reader = csv.reader(csv_data, delimiter=',')
                # csv reader object

                c = 1
                for line in enumerate(reader):

                    item = line[1]

                    try:
                        self.session.execute('insert into ccdp.training_database \
                                                ( "ID","LIMIT_BAL","SEX","EDUCATION","MARRIAGE","AGE","PAY_0","PAY_2", \
                                                "PAY_3","PAY_4","PAY_5","PAY_6","BILL_AMT1","BILL_AMT2","BILL_AMT3", \
                                                "BILL_AMT4","BILL_AMT5","BILL_AMT6","PAY_AMT1","PAY_AMT2","PAY_AMT3", \
                                                "PAY_AMT4","PAY_AMT5","PAY_AMT6","Defaulted") VALUES \
                                                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                                             [float(item[0]),
                                                 float(
                                                 item[1]),
                                                 float(
                                                 item[2]),
                                                 float(
                                                 item[3]),
                                                 float(
                                                 item[4]),
                                                 float(
                                                 item[5]),
                                                 float(
                                                 item[6]),
                                                 float(
                                                 item[7]),
                                                 float(
                                                 item[8]),
                                                 float(
                                                 item[9]),
                                                 float(
                                                 item[10]),
                                                 float(
                                                 item[11]),
                                                 float(
                                                 item[12]),
                                                 float(
                                                 item[13]),
                                                 float(
                                                 item[14]),
                                                 float(
                                                 item[15]),
                                                 float(
                                                 item[16]),
                                                 float(
                                                 item[17]),
                                                 float(
                                                 item[18]),
                                                 float(
                                                 item[19]),
                                                 float(
                                                 item[20]),
                                                 float(
                                                 item[21]),
                                                 float(
                                                 item[22]),
                                                 float(
                                                 item[23]),
                                                 float(item[24])])

                        self.logger.log(
                            log_file, "Insertion successful for record {} : line-{}" .format(file, c))
                        c = c+1
                    except Exception as e:
                        c = c+1
                        self.logger.log(
                            self.file, "Data Insertion failed. The error is: %s" % e)
                        self.logger.log(
                            log_file, "Insertion failed !!!!!!!!.For record {} : line-{}" .format(file, c))
                        continue
        self.logger.log(
            self.file, "Exiting the function of inserting_values_into_training_database")
        return None

    def insert_values_into_prediction_database(self):
        """
                        Method Name: insert_values
                        Description: This method inserts the Good data files from the Good_Raw folder into the
                                        above created table.
                        Output: None
                        On Failure: Raise Exception

                        Written By: iNeuron Intelligence
                        Edited by: Sandesh
                        Version: 2.0
                        Revisions: None

        """
        self.logger.log(
            self.file, "Attempting to insert values into training database")
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in sorted(os.listdir(goodFilePath))]
        log_file = open('Logs/database_insertion_log.txt', 'a+')

        for file in onlyfiles:

            with open(goodFilePath+'/'+file, 'r') as csv_data:
                next(csv_data)
                reader = csv.reader(csv_data, delimiter=',')
                # csv reader object

                c = 1
                for line in enumerate(reader):

                    item = line[1]

                    try:

                        self.session.execute('insert into ccdp.prediction_database \
                                                ( "ID","LIMIT_BAL","SEX","EDUCATION","MARRIAGE","AGE","PAY_0","PAY_2", \
                                                "PAY_3","PAY_4","PAY_5","PAY_6","BILL_AMT1","BILL_AMT2","BILL_AMT3", \
                                                "BILL_AMT4","BILL_AMT5","BILL_AMT6","PAY_AMT1","PAY_AMT2","PAY_AMT3", \
                                                "PAY_AMT4","PAY_AMT5","PAY_AMT6") VALUES \
                                                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', [
                            float(item[0]),
                            float(item[1]),
                            float(item[2]),
                            float(item[3]),
                            float(item[4]),
                            float(item[5]),
                            float(item[6]),
                            float(item[7]),
                            float(item[8]),
                            float(item[9]),
                            float(item[10]),
                            float(item[11]),
                            float(item[12]),
                            float(item[13]),
                            float(item[14]),
                            float(item[15]),
                            float(item[16]),
                            float(item[17]),
                            float(item[18]),
                            float(item[19]),
                            float(item[20]),
                            float(item[21]),
                            float(item[22]),
                            float(item[23]),

                        ])

                        self.logger.log(
                            log_file, "Insertion successful for record {} : line-{}" .format(file, c))
                        c = c+1
                    except Exception as e:
                        c = c+1
                        self.logger.log(
                            log_file, "Insertion failed !!!!!!!!.For record {} : line-{}" .format(file, c))
                        continue
        self.logger.log(
            self.file, "Exiting the function of inserting_values_into_prediciton_database")
        return None

    def get_table(self, process="training"):
        """
                        Method Name: get_table
                        Description: This method displays the data present in the table
                        Output: Dataframe containing data
                        On Failure: log error


                        Written by: Sandesh
                        Version: 1.0
                        Revisions: None

        """
        self.logger.log(
            self.file, "Attempting to retrieve {} database from the remote cassandra database".format(process))
        try:
            data = (list(self.session.execute(
                'select * from ccdp.{}_database;'.format(process))))

            df = pd.DataFrame(data)
            # Cassandra keeps messing up the order. Need to figure it out properly
            df.sort_values(by=['ID'], ascending=True, inplace=True)
            # This is the actual order of columns. Cassandra messes up this also. Should figure this out also
            if process == 'training':
                df = df[["ID", "LIMIT_BAL", "SEX", "EDUCATION", "MARRIAGE", "AGE", "PAY_0", "PAY_2",
                        "PAY_3", "PAY_4", "PAY_5", "PAY_6", "BILL_AMT1", "BILL_AMT2", "BILL_AMT3",
                         "BILL_AMT4", "BILL_AMT5", "BILL_AMT6", "PAY_AMT1", "PAY_AMT2", "PAY_AMT3",
                         "PAY_AMT4", "PAY_AMT5", "PAY_AMT6", "Defaulted"]]
            else:
                df = df[["ID", "LIMIT_BAL", "SEX", "EDUCATION", "MARRIAGE", "AGE", "PAY_0", "PAY_2",
                        "PAY_3", "PAY_4", "PAY_5", "PAY_6", "BILL_AMT1", "BILL_AMT2", "BILL_AMT3",
                         "BILL_AMT4", "BILL_AMT5", "BILL_AMT6", "PAY_AMT1", "PAY_AMT2", "PAY_AMT3",
                         "PAY_AMT4", "PAY_AMT5", "PAY_AMT6"]]
            self.logger.log(self.file, "Table retrieval successful")
            return df

        except Exception as e:

            self.logger.log(
                self.file, "Table retrieval failed. The error is: %s" % e)
        finally:
            self.logger.log(
                self.file, "Failed to retrieve database from the {} table".format(process))
        return None

    def db_to_csv(self, process='training'):
        """
                        Method Name: db_to_csv
                        Description: This method creates exports the database into a csv file
                        Output: csv containing data
                        On Failure: log error


                        Written by: Sandesh
                        Version: 1.0
                        Revisions: None

        """
        self.logger.log(
            self.file, "Attempting to write the data from {} database into a csv file for easier processing".format(process))
        try:
            data = self.get_table(process)
            data.to_csv(self.output_folder +
                        '/{}_file.csv'.format(process), index=False)
            self.logger.log(self.file, "Successfully created csv file")
            return None
        except Exception as e:
            self.logger.log(
                self.file, "Failed to create csv for {} database . The error is: {}".format(process, e))
        finally:
            self.logger.log(self.file, "Exiting get_table function")
        return None

    def delete_table(self, process='training'):
        """
                        Method Name: delete_table
                        Description: This method deletes the created table
                        On Failure: log error


                        Written by: Sandesh
                        Version: 1.0
                        Revisions: None

        """
        self.logger.log(
            self.file, "Attempting to delete {} table".format(process))
        try:
            a = 3
            # Please dont delete this table. It's very difficult to upload data
            # Don't delete ### self.session.execute('drop table ccdp."UCI_Credit_Card"') ### dont' delete
            self.session.execute('drop table ccdp.{}_database'.format(process))
            self.logger.log(
                self.file, "{} Table deletion successful:".format(process))
        except Exception as e:

            self.logger.log(
                self.file, "Failed to delete {} table. The error is: {}".format(process, e))
        finally:
            self.logger.log(self.file, "Exiting function delete_table")
        return None

    def close_cassandra(self):
        """
                        Method Name: close_cassandra
                        Description: This method is used to clode the cassandra and self.session and disconnect from the server
                        On Failure: log error


                        Written by: Sandesh
                        Version: 1.0
                        Revisions: None

        """
        self.logger.log(
            self.file, "Attempting to close the connection between computer and cassandra database")
        try:
            self.session.shutdown()

            self.logger.log(self.file, "Shutdown successful")
        except Exception as e:

            self.logger.log(
                self.file, "Shutdown failed. The error is : %s" % e)
        finally:
            self.logger.log(self.file, "Exiting the close_cassandra function")

        return None
