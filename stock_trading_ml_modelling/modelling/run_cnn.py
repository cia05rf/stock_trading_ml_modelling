"""Script to create the training data

GOAL
----
To create a classifier which will identify mins and max points in price charts
"""
from numpy import argmax

from stock_trading_ml_modelling.utils.log import logger

from stock_trading_ml_modelling.modelling.data_builder import DataBuilder
from stock_trading_ml_modelling.modelling.classifier_model import ClassifierModel

folder = "new_cnn"
ticker_ids = list(range(10))
years = 10
refresh = False

############################
### CREATE TRAINING DATA ###
############################
training_data = DataBuilder(
    ticker_ids=ticker_ids,
    folder=folder
)
if refresh:
    #Create new data
    training_data.create_data(weeks=52*years, force=True)
    training_data.save_data()
#Use existing data
training_data.load_data()

#########################
### TRAIN MACRO MODEL ###
#########################
"""Create a clalable which a model can be input as a handler and predictions 
will be output.

Data will be split between training and validation through random sampling.

All model must have:
- Split data
- Train
- Save
- Run
methods
"""
cls_model = ClassifierModel(3, folder=folder)
if refresh:
    cls_model.train(training_data.X_train, training_data.y_train, training_data.labels)
    cls_model.save_model()
    cls_model.eval_model(training_data.X_test, training_data.y_test, training_data.labels)

#########################################
### RUN VALIDATION DATA THROUGH MODEL ###
#########################################
"""Run the validation set through the model and then run the output from that 
through a simulation to find profit/loss of the model"""
#Load
cls_model.load_model()

preds = cls_model.predict_sml(training_data.X_test)
preds = argmax(preds, axis=1)
training_data.save_preds(preds)

#Evaluate the model
val_loss, val_acc = cls_model.evaluate(
    training_data.X_test,
    training_data.y_test
    )
print(f"val_loss, val_acc -> {val_loss, val_acc}")

############################################
### CREATE MODELS WITH TRANSFER LEARNING ###
############################################
#Get the data
folder = "new_cnn"
ticker_ids = [1]
years = 10
tick_data = DataBuilder(
    ticker_ids=ticker_ids,
    folder=folder
)
tick_data.create_data(weeks=52*years, force=True)

#Load the base_model
cls_model = ClassifierModel(3, folder=folder)
cls_model.load_model()

#Instantiate the model
import tensorflow as tf
x = cls_model.base_model(3, training=False)

#Remove the last softmax layer

#Re-train the softmax layer
prediction_layer = tf.keras.layers.Dense(3, activation='softmax')
outputs = prediction_layer(x)

