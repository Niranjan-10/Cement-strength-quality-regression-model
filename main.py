from flask import Flask, request, render_template
import os
from flask import Response
from flask_cors import CORS,cross_origin
from training_Model import trainModel
from training_Validation_Insertion import trainValidation
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction
from wsgiref import simple_server

app = Flask(__name__)



@app.route('/', methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')




@app.route('/predict', methods=['POST'])
@cross_origin()
def predict():
    try:
        req_data = request.get_json()

        if req_data['folder_path'] is not None:
            path = req_data['folder_path']
            pred_val = pred_validation(path)
            pred_val.prediction_validation()
            pred = prediction(path) #object initialization
            path = pred.predictionFromModel()
            return Response("Prediction File created at %s!!!" % path)
        elif request.form is not None:
            print(request)
            path = request.form['filepath']
            print(path)

            pred_val = pred_validation(path) #object initialization

            pred_val.prediction_validation() #calling the prediction_validation function

            pred = prediction(path) #object initialization
            print(pred)

            # predicting for dataset present in database
            path = pred.predictionFromModel()
            print(path)
            # print('excuted')
            return Response("Prediction File created at %s!!!" % path)
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        print(e)
        return Response("Error Occurred! %s")









@app.route('/train', methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        if request.method == 'POST':
            req_data = request.get_json()

            if req_data['folder_path'] is not None:
                path  = req_data['folder_path']
                # print(path)
                train_valobj = trainValidation(path)  #trainvalidation object is created
                # print(train_valobj)
                train_valobj.train_validation()  #calling the training_validation function

                trainModelObj = trainModel() #object initialization
                trainModelObj.trainingModel() #training the model for the files in the table

                  
    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:
        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

    #if port exist in enironment then use that port or use 50001
if __name__ == '__main__':
    # host='127.0.0.1'
    port = int(os.getenv("PORT", 8080))
    # app.run(host='0.0.0.0', port=port)
    httpd = simple_server.make_server('127.0.0.1',port, app)
    print("Serving on %s:%d" % ( '127.0.0.1',port))
    httpd.serve_forever()






