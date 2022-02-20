import os
import json


class read_schema():
    """
        This class shall be used to read the schema shared by the client. Deatiles of the data names, type are described in the schema.

        Written By: Sandesh
        Version: 1.0
        Revisions: None
    """

    def __init__(self, schema_path='schema_training.json'):
        self.schema_path = 'schema_training.json'

    def read_json(self, schema_path='schema_training.json'):
        """
                Method Name: read_json
                Description:  This method shall be used to read the schema shared by the client. Deatiles of the data names, type are described in the schema.
                Output: Returns Dictionary. Dictonary containing the details od data conventions is returned

                Written by: Sandesh
                Version :1
                Revisions : None
        """
        with open(schema_path, 'r') as f:
            dic = json.load(f)
            f.close()

        return dic
