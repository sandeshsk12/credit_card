import numpy as np
from flask import Flask, request, jsonify, render_template, send_file
import pickle
import pandas as pd
from flask_wtf import FlaskForm
from wtforms import FileField, SubmitField
from werkzeug.utils import secure_filename
import os
from wtforms.validators import InputRequired


app = Flask(__name__)
app.config['SECRET_KEY'] = 'supersecretkey'
app.config['UPLOAD_FOLDER'] = 'static/files'
model = pickle.load(open('models/trained_model', 'rb'))


class UploadFileForm(FlaskForm):
    file = FileField("File", validators=[InputRequired()])
    submit = SubmitField("Upload File")


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    form = UploadFileForm()
    if form.validate_on_submit():
        file = form.file.data  # First grab the file
        file.save(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                  app.config['UPLOAD_FOLDER'], secure_filename(file.filename)))  # Then save the file
        return "File has been uploaded."

    data = pd.read_csv('prediction_files_from_db/prediction_file.csv')

    y = []
    for i in range(len(data)):
        y.append(model.predict([data.iloc[i, 1:].to_list()])[0])

    data['defaulted'] = y
    data.to_csv('predicted_files/predicted_file.csv', index=False)
    int_features = []

    for x in request.form.values():
        try:
            int_features.append(int(x))
        except:
            pass

    final_features = [np.array(int_features)]
    print(int_features)
    output = 'will default' if (model.predict(
        final_features)) == 1 else "won't default"

    return render_template('index.html', prediction_text='The customer {}'.format(output), form=form)


@app.route('/download')
def download_file():
    p = 'predicted_files/predicted_file.csv'
    return send_file(p, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
