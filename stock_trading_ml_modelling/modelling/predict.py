from numpy import argmax

from stock_trading_ml_modelling.modelling.data_builder import DataBuilder
from stock_trading_ml_modelling.modelling.classifier_model import ClassifierModel

folder = "new_cnn"
limit_id = 10
years = 2

#Build data for prediction
training_data = DataBuilder(
    folder=folder,
    limit_id=10
)
training_data.get_price_data(weeks=52*years)
training_data.load_data()

#Predict
cls_model = ClassifierModel(3, folder="cnn")
cls_model.load_model()

preds = cls_model.predict_sml(training_data.X_test)
preds = argmax(preds, axis=1)
preds, _ = training_data.decode_labels(preds)

#Join preditions with original data
predicted_prices = training_data.prices.copy()
predicted_prices = predicted_prices.set_index("id")
predicted_prices = predicted_prices.loc[
    training_data.prices_id_test, :
]
predicted_prices["signal"] = preds

#Export data with predictions