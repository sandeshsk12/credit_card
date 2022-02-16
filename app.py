import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle
from Database_operation.cassandra_data_op import cassandra_ops
import pandas as pd
from File_operations.file_ops import file_operations
from train_valid import train_validation
from train_model import trainmodel
from pred_valid import pred_validation
from prediction import predict_data
from flask import Response


app = Flask(__name__)

model = pickle.load(open('models/trained_model', 'rb'))


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    int_features = [int(x) for x in request.form.values()]
    final_features = [np.array(int_features)]
    print(int_features)
    output = model.predict(final_features)

    return render_template('index.html', prediction_text='Output should be $ {}'.format(output))


if __name__ == "__main__":
    app.run(debug=True)
