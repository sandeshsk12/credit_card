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
    a = 2
    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
