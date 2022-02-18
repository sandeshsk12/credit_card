import numpy as np
from flask import Flask, request, jsonify, render_template, send_file, redirect, flash, redirect, url_for
import pickle
import pandas as pd

from werkzeug.utils import secure_filename
import os


app = Flask(__name__)
model = pickle.load(open('models/trained_model', 'rb'))

app.config["FILE_UPLOADS"] = "prediction_files_from_db"
app.config["ALLOWED_FILE_EXTENSIONS"] = ["JPEG", "JPG", "PNG", "GIF", "CSV"]
app.config["MAX_file_FILESIZE"] = 0.5 * 1024 * 1024


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

    return render_template('index.html', prediction_text='The customer {}'.format(output))


@app.route('/download')
def download_file():
    p = 'predicted_files/predicted_file.csv'
    return send_file(p, as_attachment=True)


@app.route("/upload-file", methods=["GET", "POST"])
def upload_file():

    if request.method == "POST":

        if request.files:

            file = request.files["file"]

            if file.filename == "":
                print("No filename")
                return redirect(request.url)

            if allowed_file(file.filename):
                filename = secure_filename(file.filename)

                file.save(os.path.join(app.config["FILE_UPLOADS"], filename))

                print("file saved")
                data = pd.read_csv(
                    'prediction_files_from_db/prediction_file.csv')

                y = []
                for i in range(len(data)):
                    y.append(model.predict([data.iloc[i, 1:].to_list()])[0])

                data['defaulted'] = y
                data.to_csv('predicted_files/predicted_file.csv', index=False)

                return redirect(request.url)

            else:
                print("That file extension is not allowed")
                return redirect(request.url)

    return render_template('index.html')


def allowed_file(filename):

    # We only want files with a . in the filename
    if not "." in filename:
        return False

    # Split the extension from the filename
    ext = filename.rsplit(".", 1)[1]

    # Check if the extension is in ALLOWED_file_EXTENSIONS
    if ext.upper() in app.config["ALLOWED_FILE_EXTENSIONS"]:
        return True
    else:
        return False


if __name__ == "__main__":
    app.run(debug=True)
