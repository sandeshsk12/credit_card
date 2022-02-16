import numpy as np
from flask import Flask, request, jsonify, render_template, send_file
import pickle
import pandas as pd


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

    data = pd.read_csv('prediction_files_from_db/prediction_file.csv')

    y = []
    for i in range(len(data)):
        y.append(model.predict([data.iloc[i, 1:].to_list()])[0])

    data['defaulted'] = y
    data.to_csv('predicted_files/predicted_file.csv', index=False)

    int_features = [int(x) for x in request.form.values()]
    final_features = [np.array(int_features)]
    print(int_features)
    output = 'will default' if (model.predict(final_features)) == 1 else 0

    return render_template('index.html', prediction_text='The customer {}'.format(output))


@app.route('/download')
def download_file():
    p = 'predicted_files/predicted_file.csv'
    return send_file(p, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
