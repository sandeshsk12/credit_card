import os
import json

class read_schema():
    def __init__(self,schema_path='schema_training.json'):
        self.schema_path='schema_training.json'

    def read_json(self,schema_path='schema_training.json'):
        with open(schema_path,'r') as f:
            dic=json.load(f)
            f.close()
        #print(dic)
        return dic


data_dic=read_schema().read_json('schema_prediction.json')
column_names=data_dic['ColName']
print(column_names)

process='train'

if process=='pred':
                        
        data_dic=read_schema().read_json('schema_prediction.json')
        column_names=data_dic['ColName']
else:
        
        data_dic=read_schema().read_json('schema_training.json')
        column_names=data_dic['ColName']
print(str(list(data_dic['ColName'].keys())).replace("'",''))
#col_list=[ str(x)+' '+str(data_dic['ColName'][x]) for x in column_names.keys()]
#print(str(col_list).replace('[','(').replace(']','').replace("'",'')+str(',primary key(ID))'))