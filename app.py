from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
from train_valid import train_validation
from train_model import trainmodel
from pred_valid import pred_validation
from prediction import predict_data


app = Flask(__name__)


@app.route("/")
def home():
    return render_template('index.html')


@app.route("/predict", methods=['POST'])
def predict():
    pred_val = pred_validation()  # object initialization

    pred_val.pred_validation()  # calling the prediction_validation function

    pred = predict_data()  # object initialization

    # predicting for dataset present in database
    path = pred.predict_data()
    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
