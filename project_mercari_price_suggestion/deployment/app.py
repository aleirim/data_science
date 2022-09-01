from flask import Flask, request, url_for, redirect, render_template, jsonify
import pandas as pd
import pickle
import numpy as np
import preprocessing


app = Flask(__name__, template_folder = 'templates')

# loading model from picke file
with open(f'model/stack_regressor.pkl', 'rb') as m:
    model = pickle.load(m)
# cols for df
cols = ['name', 'item_condition_id', 'category_name', 'brand_name', 'shipping', 'item_description']

@app.route('/')
def main():
    return render_template("main.html")

@app.route('/new2')
def index2():
    return render_template("index2.html")

@app.route('/predict',methods=['POST'])
def predict():
    form_data = [x for x in request.form.values()]
    data_array = np.array(form_data)
    data_unseen = pd.DataFrame([data_array], columns = cols)
    # preprocessing data for the model
    data_model = preprocessing.preprocess_data(data_unseen)

    print("---------- Predicting price ----------")
    prediction = model.predict(data_model)
    prediction = round(preprocessing.get_pred_price(prediction[0]), 2)
    return render_template('main.html', pred='Suggested Price {}'.format(prediction))

# @app.route('/predict_api',methods=['POST'])
# def predict_api():
#     data = request.get_json(force=True)
#     data_unseen = pd.DataFrame([data])
#     prediction = predict_model(model, data=data_unseen)
#     output = prediction.Label[0]
#     return jsonify(output)


if __name__ == '__main__':
    app.run(debug=True)