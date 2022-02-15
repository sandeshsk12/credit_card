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
    try:
        if request.json is not None:
            path = request.json['filepath']

            pred_val = pred_validation(path)  # object initialization

            pred_val.pred_validation()  # calling the prediction_validation function

            pred = predict_data(path)  # object initialization

            # predicting for dataset present in database
            path = pred.predict_data()
            return render_template('index.html')
        elif request.form is not None:
            path = request.form['filepath']

            pred_val = pred_validation(path)  # object initialization

            pred_val.pred_validation()  # calling the prediction_validation function

            pred = predict_data()  # object initialization

            # predicting for dataset present in database
            path = pred.predict_data()
            return render_template('index.html')
        else:
            print('Nothing Matched')
    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)


@app.route("/train", methods=['POST'])
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']

            train_valObj = train_validation()  # object initialization

            train_valObj.train_validation()  # calling the training_validation function

            trainModelObj = trainmodel()  # object initialization
            trainModelObj.train_model()  # training the model for the files in the table

    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")


if __name__ == "__main__":
    app.run(debug=True)
