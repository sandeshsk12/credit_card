from datetime import datetime


class App_Logger:
    """
    This class shall be used to perform logging. Instead of calling the default function everytime, we write a custom class to help define 
    the structure once and use the same structure for all subsequent logging. 

    Written By: Sandesh
    Version: 1.0
    Revisions: None
    """

    def __init__(self):
        pass

    def log(self, file_object, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        file_object.write(
            str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message + "\n")
