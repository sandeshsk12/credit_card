import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle


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
    output = 'will default' if (model.predict(final_features)) == 1 else 0

    return render_template('index.html', prediction_text='The customer {}'.format(output))


if __name__ == "__main__":
    app.run(debug=True)
