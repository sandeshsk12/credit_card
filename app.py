import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle

import pandas as pd

from flask import Response


app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI

    a = train_validation()
    a.train_validation()

    b = trainmodel()
    b.train_model()
    c = pred_validation()
    c.pred_validation()

    d = predict_data()
    d.predict_data()
    '''
    a = 2

    return Response("Prediction File created !!!")


if __name__ == "__main__":
    app.run(debug=True)
