import os
import json

class read_schema():
    def __init__(self):
        self.schema_path='schema_training.json'

    def read_json(self):
        with open(self.schema_path,'r') as f:
            dic=json.load(f)
            f.close()
        #print(dic)
        return dic
