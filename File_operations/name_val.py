from importlib.resources import path
import os
import re
import shutil


class validate_file_name():


    def __init__(self):
        self.path=path
        self.pattern='(creditCardFraud)[_](\d\d\d\d\d\d\d\d)[_](\d\d\d\d\d\d)[.](csv)'
        self.match=None
        self.good_folder='good_files'
        self.bad_folder='good_files'

    def check_name(self,file_name):
        self.match=(re.match(self.pattern,file_name))
        if self.match!=None:
            #shutil.copy(file_name,self.good_folder)
            return True
        else:
            return False
