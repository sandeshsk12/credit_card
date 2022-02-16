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

model = pickle.load(open('model.pkl', 'rb'))


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
    prediction = model.predict(final_features)

    output = round(prediction[0], 2)
    a = train_validation()
    a.train_validation()

    b = trainmodel()
    b.train_model()
    c = pred_validation()
    c.pred_validation()

    d = predict_data()
    d.predict_data()

    return render_template('index.html', prediction_text='Employee Salary should be $ {}'.format(output))


if __name__ == "__main__":
    app.run(debug=True)
