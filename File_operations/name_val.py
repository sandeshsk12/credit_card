from importlib.resources import path
import os
import re
import shutil


class validate_file_name():
    """
        This class shall be used to check if the filename is according the data sharing agreement
        An example of good file name are - "creditCardFraud_28011960_120210.csv"

        Written By: Sandesh
        Version: 1.0
        Revisions: None
    """

    def __init__(self):

        self.path = path
        self.pattern = '(creditCardFraud)[_](\d\d\d\d\d\d\d\d)[_](\d\d\d\d\d\d)[.](csv)'
        self.match = None
        self.good_folder = 'good_files'
        self.bad_folder = 'good_files'

    def check_name(self, file_name):
        """
                Method Name: check_name
                Description:  This method shall be used to check if the filename is according the data sharing agreement
                                An example of good file name are - "creditCardFraud_28011960_120210.csv"
                Output: Returns Boolean. True if the filename is according the convention and False if the filename is not according to the convention

                Written by: Sandesh
                Version :1
                Revisions : None
        """
        self.match = (re.match(self.pattern, file_name))
        if self.match != None:

            return True
        else:
            return False
