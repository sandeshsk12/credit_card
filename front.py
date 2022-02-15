
import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle
from application_logging.logger import App_Logger
logger = App_Logger()

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('Form.html')


@app.route('/predict', methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    int_features = [int(x) for x in request.form.values()]
    log_file = open('front.txt', 'a+')
    logger.log(log_file, "Checking and imputing NULL where data is missing")

    return render_template('Form.html', prediction_text='Employee Salary should be $ {}'.format('a'))


if __name__ == "__main__":
    app.run(debug=True)
