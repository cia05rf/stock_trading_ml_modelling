
import numpy as np
from pathlib import Path
from tensorflow.keras.metrics import SparseCategoricalAccuracy
from tensorflow.keras.losses import SparseCategoricalCrossentropy
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import load_model

from stock_trading_ml_modelling.utils.data import np_count_values

from stock_trading_ml_modelling.modelling.resnet_model import FunnyResNet

class ClassifierModel:
    def __init__(self,
        num_classes:int,
        learning_rate:float=1e-5,
        loss:callable=SparseCategoricalCrossentropy,
        folder:str="default",
        epochs:int=50,
        validation_split:float=0.2,
        batch_size:int=32
        ):
        self.learning_rate = learning_rate
        self.base_model = FunnyResNet(num_classes)
        self.loss = loss
        self.compiled = False
        self.path = Path("data", "models", folder)
        self.epochs = epochs
        self.validation_split = validation_split
        self.batch_size = batch_size

    def train(self,
        X,
        y,
        labels
        ):
        self.compile()
        self.fit(X, y, labels=labels)

    def compile(self):
        self.base_model.compile(
            optimizer=Adam(learning_rate=self.learning_rate),
            loss="sparse_categorical_crossentropy",
            metrics=[
                SparseCategoricalAccuracy(name="sca")
                ]
        )
        self.compiled = True

    def create_class_weighting(self, y, labels:dict):
        return {
            v:1 - (y == v).sum() / y.shape[0]
            for k,v in labels.items()
            }

    def fit(self,
        X,
        y,
        labels
        ):
        if not self.compiled:
            self.compile()
        #Add early stopping
        early_stopping = EarlyStopping()
        #Create the loss weightings
        class_weight = self.create_class_weighting(y, labels)
        print(f"class_weight -> {class_weight}")
        #Fit the model
        self.base_model.fit(
            X,
            y,
            epochs=self.epochs,
            validation_split=self.validation_split,
            batch_size=self.batch_size,
            callbacks=[early_stopping],
            class_weight=class_weight
            )

    def save_model(self):
        self.base_model.save(self.path)

    def load_model(self):
        self.base_model = load_model(self.path)
        self.compiled = True

    def eval_model(self, X, y, labels:dict):
        #Evaluate the model
        val_loss, val_acc = self.base_model.evaluate(X, y)
        print(f"val_loss:{val_loss} - val_acc:{val_acc}")

        preds = self.base_model.predict(X)
        preds = np.argmax(preds, axis=1)

        act_val_counts = np_count_values(y)
        pred_val_counts = np_count_values(preds)
        print(f"act_val_counts ->\n{act_val_counts}")
        print(f"pred_val_counts ->\n{pred_val_counts}")

        #PPV
        for k,v in labels.items():
            mask = preds == v
            this_preds = preds[mask]
            act = y[mask]
            tp = (act == this_preds).sum()
            fp = (act != this_preds).sum()
            ppv = tp / (tp + fp)
            print(f"ppv of {k} - {ppv:.4f} - tp {tp} - fp {fp} - tp + fn {(y == v).sum()}")

    def predict_sml(self, X, *args, **kwargs):
        return self.base_model(X, training=False, *args, **kwargs)

    def predict(self, X, *args, **kwargs):
        return self.base_model.predict(X, *args, **kwargs)

    def evaluate(self, X, y, *args, **kwargs):
        return self.base_model.evaluate(X, y, *args, **kwargs)